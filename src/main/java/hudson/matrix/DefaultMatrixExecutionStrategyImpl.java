package hudson.matrix;

import com.google.common.collect.Iterables;
import groovy.lang.GroovyRuntimeException;
import hudson.AbortException;
import hudson.Extension;
import hudson.console.ModelHyperlinkNote;
import hudson.matrix.MatrixBuild.MatrixBuildExecution;
import hudson.matrix.listeners.MatrixBuildListener;
import hudson.model.Action;
import hudson.model.BuildListener;
import hudson.model.Cause.UpstreamCause;
import hudson.model.ParametersAction;
import hudson.model.Queue;
import hudson.model.ResourceController;
import hudson.model.Result;
import hudson.model.Run;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nullable;

import jenkins.model.Jenkins;
import org.kohsuke.stapler.DataBoundConstructor;

/**
 * {@link MatrixExecutionStrategy} that captures historical behavior.
 *
 * <p>
 * This class is somewhat complex because historically this wasn't an extension point and so
 * people tried to put various logics that cover different use cases into one place.
 * Going forward, people are encouraged to create subtypes to implement a custom logic that suits their needs.
 *
 * @author Kohsuke Kawaguchi
 * @since 1.456
 */
public class DefaultMatrixExecutionStrategyImpl extends MatrixExecutionStrategy {
    private static ExecutorService cachedPool;
    private volatile boolean runSequentially;

    /**
     * Filter to select a number of combinations to build first
     */
    private volatile String touchStoneCombinationFilter;

    /**
     * Required result on the touchstone combinations, in order to
     * continue with the rest
     */
    private volatile Result touchStoneResultCondition;

    private volatile MatrixConfigurationSorter sorter;

    @DataBoundConstructor
    public DefaultMatrixExecutionStrategyImpl(Boolean runSequentially, boolean hasTouchStoneCombinationFilter, String touchStoneCombinationFilter, Result touchStoneResultCondition, MatrixConfigurationSorter sorter) {
        this(runSequentially!=null ? runSequentially : false,
            hasTouchStoneCombinationFilter ? touchStoneCombinationFilter : null,
            hasTouchStoneCombinationFilter ? touchStoneResultCondition : null,
            sorter);
    }

    public DefaultMatrixExecutionStrategyImpl(boolean runSequentially, String touchStoneCombinationFilter, Result touchStoneResultCondition, MatrixConfigurationSorter sorter) {
        this.runSequentially = runSequentially;
        this.touchStoneCombinationFilter = touchStoneCombinationFilter;
        this.touchStoneResultCondition = touchStoneResultCondition;
        this.sorter = sorter;
    }

    public DefaultMatrixExecutionStrategyImpl() {
        this(false,false,null,null,null);
    }

    public boolean getHasTouchStoneCombinationFilter() {
        return touchStoneCombinationFilter!=null;
    }

    /**
     * If true, {@link MatrixRun}s are run sequentially, instead of running in parallel.
     *
     * TODO: this should be subsumed by {@link ResourceController}.
     */
    public boolean isRunSequentially() {
        return runSequentially;
    }

    public void setRunSequentially(boolean runSequentially) {
        this.runSequentially = runSequentially;
    }

    public String getTouchStoneCombinationFilter() {
        return touchStoneCombinationFilter;
    }

    public void setTouchStoneCombinationFilter(String touchStoneCombinationFilter) {
        this.touchStoneCombinationFilter = touchStoneCombinationFilter;
    }

    public Result getTouchStoneResultCondition() {
        return touchStoneResultCondition;
    }

    public void setTouchStoneResultCondition(Result touchStoneResultCondition) {
        this.touchStoneResultCondition = touchStoneResultCondition;
    }

    public MatrixConfigurationSorter getSorter() {
        return sorter;
    }

    public void setSorter(MatrixConfigurationSorter sorter) {
        this.sorter = sorter;
    }

    @Override
    public Result run(MatrixBuildExecution execution) throws InterruptedException, IOException {

        Collection<MatrixConfiguration> touchStoneConfigurations = new HashSet<MatrixConfiguration>();
        Collection<MatrixConfiguration> delayedConfigurations = new HashSet<MatrixConfiguration>();

        filterConfigurations(
                execution,
                touchStoneConfigurations,
                delayedConfigurations
        );

        if (notifyStartBuild(execution.getAggregators())) return Result.FAILURE;

        if (sorter != null) {
            touchStoneConfigurations = createTreeSet(touchStoneConfigurations, sorter);
            delayedConfigurations    = createTreeSet(delayedConfigurations, sorter);
        }

        if(!runSequentially)
            for(MatrixConfiguration c : touchStoneConfigurations)
                scheduleConfigurationBuild(execution, c);

        PrintStream logger = execution.getListener().getLogger();

        Result r = Result.SUCCESS;
        for (MatrixConfiguration c : touchStoneConfigurations) {
            if(runSequentially)
                scheduleConfigurationBuild(execution, c);
            MatrixRun run = waitForCompletion(execution, c);
            notifyEndBuild(run,execution.getAggregators());
            logger.println(Messages.MatrixBuild_Completed(ModelHyperlinkNote.encodeTo(c), getResult(run)));
            r = r.combine(getResult(run));
        }

        if (touchStoneResultCondition != null && r.isWorseThan(touchStoneResultCondition)) {
            logger.printf("Touchstone configurations resulted in %s, so aborting...%n", r);
            return r;
        }

        if(!runSequentially) {
            int maxInQueue = 0;

            if(delayedConfigurations.size() != 0){
                maxInQueue = Iterables.get(delayedConfigurations, 0).asProject().getParent().getMatrixThrottleMaxBuilds();
            }

            if(maxInQueue == 0){
                for (MatrixConfiguration c : delayedConfigurations) {
                    scheduleConfigurationBuild(execution, c);
                }
            }else{
                startCallable(new StartBuildsCallable(execution, delayedConfigurations));
            }

        }

        for (MatrixConfiguration c : delayedConfigurations) {
            if(runSequentially)
                scheduleConfigurationBuild(execution, c);
            MatrixRun run = waitForCompletion(execution, c);
            notifyEndBuild(run,execution.getAggregators());
            logger.println(Messages.MatrixBuild_Completed(ModelHyperlinkNote.encodeTo(c), getResult(run)));
            r = r.combine(getResult(run));
        }

        return r;
    }

    private void filterConfigurations(
            final MatrixBuildExecution execution,
            final Collection<MatrixConfiguration> touchStoneConfigurations,
            final Collection<MatrixConfiguration> delayedConfigurations
    ) throws AbortException {

        final MatrixBuild build = execution.getBuild();

        final FilterScript combinationFilter = FilterScript.parse(execution.getProject().getCombinationFilter(), FilterScript.ACCEPT_ALL);
        final FilterScript touchStoneFilter = FilterScript.parse(getTouchStoneCombinationFilter(), FilterScript.REJECT_ALL);

        try {

            for (MatrixConfiguration c: execution.getActiveConfigurations()) {

                if (!MatrixBuildListener.buildConfiguration(build, c)) continue; // skip rebuild

                final Combination combination = c.getCombination();

                if (touchStoneFilter != null && touchStoneFilter.apply(execution, combination)) {
                    touchStoneConfigurations.add(c);
                } else if (combinationFilter.apply(execution, combination)) {
                    delayedConfigurations.add(c);
                }
            }
        } catch (GroovyRuntimeException ex) {

            PrintStream logger = execution.getListener().getLogger();
            logger.println(ex.getMessage());
            ex.printStackTrace(logger);
            throw new AbortException("Failed executing combination filter");
        }
    }

    private Result getResult(@Nullable MatrixRun run) {
        // null indicates that the run was cancelled before it even gets going
        return run!=null ? run.getResult() : Result.ABORTED;
    }

    private boolean notifyStartBuild(List<MatrixAggregator> aggregators) throws InterruptedException, IOException {
        for (MatrixAggregator a : aggregators)
            if(!a.startBuild())
                return true;
        return false;
    }

    private void notifyEndBuild(MatrixRun b, List<MatrixAggregator> aggregators) throws InterruptedException, IOException {
        if (b==null)    return; // can happen if the configuration run gets cancelled before it gets started.
        for (MatrixAggregator a : aggregators)
            if(!a.endRun(b))
                throw new AbortException();
    }
    
    private <T> TreeSet<T> createTreeSet(Collection<T> items, Comparator<T> sorter) {
        TreeSet<T> r = new TreeSet<T>(sorter);
        r.addAll(items);
        return r;
    }

    /** Function to start schedule a single configuration
     *
     * This function schedule a build of a configuration passing all of the Matrixchild actions
     * that are present in the parent build.
     *
     * @param exec  Matrix build that is the parent of the configuration
     * @param c     Configuration to schedule
     */
    private void scheduleConfigurationBuild(MatrixBuildExecution exec, MatrixConfiguration c) {
        MatrixBuild build = exec.getBuild();
        exec.getListener().getLogger().println(Messages.MatrixBuild_Triggering(ModelHyperlinkNote.encodeTo(c)));

        // filter the parent actions for those that can be passed to the individual jobs.
        List<Action> childActions = new ArrayList<Action>(build.getActions(MatrixChildAction.class));
        childActions.addAll(build.getActions(ParametersAction.class)); // used to implement MatrixChildAction
        c.scheduleBuild(childActions, new UpstreamCause((Run)build));
    }

    private MatrixRun waitForCompletion(MatrixBuildExecution exec, MatrixConfiguration c) throws InterruptedException, IOException {
        BuildListener listener = exec.getListener();
        String whyInQueue = "";
        long startTime = System.currentTimeMillis();

        // wait for the completion
        int appearsCancelledCount = 0;
        while(true) {
            MatrixRun b = c.getBuildByNumber(exec.getBuild().getNumber());

            // two ways to get beyond this. one is that the build starts and gets done,
            // or the build gets cancelled before it even started.
            if(b!=null && !b.isBuilding()) {
                Result buildResult = b.getResult();
                if(buildResult!=null)
                    return b;
            }
            Queue.Item qi = c.getQueueItem();
            if(b==null && qi==null)
                appearsCancelledCount++;
            else
                appearsCancelledCount = 0;

            if(appearsCancelledCount>=5) {
                // there's conceivably a race condition in computating b and qi, as their computation
                // are not synchronized. There are indeed several reports of Hudson incorrectly assuming
                // builds being cancelled. See
                // http://www.nabble.com/Master-slave-problem-tt14710987.html and also
                // http://www.nabble.com/Anyone-using-AccuRev-plugin--tt21634577.html#a21671389
                // because of this, we really make sure that the build is cancelled by doing this 5
                // times over 5 seconds
                listener.getLogger().println(Messages.MatrixBuild_AppearsCancelled(ModelHyperlinkNote.encodeTo(c)));
                return null;
            }

            if(qi!=null) {
                // if the build seems to be stuck in the queue, display why
                String why = qi.getWhy();
                if(why != null && !why.equals(whyInQueue) && System.currentTimeMillis()-startTime>5000) {
                    listener.getLogger().print("Configuration " + ModelHyperlinkNote.encodeTo(c)+" is still in the queue: ");
                    qi.getCauseOfBlockage().print(listener); //this is still shown on the same line
                    whyInQueue = why;
                }
            }
            
            Thread.sleep(1000);
        }
    }

    private static Future startCallable(Callable callable){
        //Share executor pool for all Matrix builds instead of creating a new cached thread pool
        //Every time a new build is created
        if (cachedPool == null){
            cachedPool = Executors.newCachedThreadPool();
        }

        return cachedPool.submit(callable);

    }

    private class StartBuildsCallable implements Callable{

        private final MatrixBuildExecution execution;
        private final Collection<MatrixConfiguration> configurations;
        private int sleepInterval = 1000;
        private int maxInQueue = 0; //Set to default to no throttle just in case


        public StartBuildsCallable(MatrixBuildExecution execution, Collection<MatrixConfiguration> configurations){
            this.configurations = configurations;
            this.execution = execution;

            this.maxInQueue = Iterables.get(this.configurations, 0).asProject().getParent().getMatrixThrottleMaxBuilds();
            this.sleepInterval = Iterables.get(this.configurations, 0).asProject().getParent().getMatrixThrottleDelay();
        }

        @Override
        public Object call() throws Exception {
            for (MatrixConfiguration currentConfiguration : this.configurations) {
                while(!timeToAddToQueue(this.maxInQueue, this.configurations)){
                    Thread.sleep(this.sleepInterval);
                }
                scheduleConfigurationBuild(this.execution, currentConfiguration);

            }
            return null;
        }

        protected boolean timeToAddToQueue(int maxInQueue, Collection<MatrixConfiguration> configurations){
            return maxInQueue == 0 || buildsCurrentlyInQueueForThisProject(configurations) < maxInQueue;
        }


        protected int buildsCurrentlyInQueueForThisProject(Collection<MatrixConfiguration> configurations){
            List<Queue.BuildableItem> buildableItems = Jenkins.getInstance().getQueue().getBuildableItems();
            List<MatrixConfiguration> allMatrixConfigurationInQueue = new LinkedList<MatrixConfiguration>();

            for (Queue.BuildableItem currentBuildableItem : buildableItems) {
                if(currentBuildableItem.task.getOwnerTask() instanceof MatrixConfiguration){
                    //Ignore any non matrix build types
                    allMatrixConfigurationInQueue.add((MatrixConfiguration) currentBuildableItem.task.getOwnerTask());
                }
            }

            int totalInQueue = 0;
            for(MatrixConfiguration configuration : configurations){
                if (allMatrixConfigurationInQueue.contains(configuration)){
                    totalInQueue++;
                }
            }


            return totalInQueue;
        }
    }

    @Extension
    public static class DescriptorImpl extends MatrixExecutionStrategyDescriptor {
        @Override
        public String getDisplayName() {
            return "Classic";
        }
    }
}
