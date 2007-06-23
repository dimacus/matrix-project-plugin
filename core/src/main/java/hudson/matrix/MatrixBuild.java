package hudson.matrix;

import hudson.model.AbstractBuild;
import hudson.model.BuildListener;
import hudson.model.Result;

import java.io.IOException;
import java.io.PrintStream;

/**
 * @author Kohsuke Kawaguchi
 */
public class MatrixBuild extends AbstractBuild<MatrixProject,MatrixBuild> {
    public MatrixBuild(MatrixProject job) throws IOException {
        super(job);
    }

    @Override
    public void run() {
        run(new RunnerImpl());
    }

    private class RunnerImpl extends AbstractRunner {
        protected Result doRun(BuildListener listener) throws Exception {
            MatrixProject p = getProject();
            PrintStream logger = listener.getLogger();

            for(MatrixConfiguration c : p.getActiveConfigurations()) {
                logger.println("Triggering "+c.getName());
                c.scheduleBuild();
            }

            // TODO: wait for completion
            
            return Result.SUCCESS;
        }

        public void post(BuildListener listener) {
            // TODO: run aggregators
        }
    }
}
