package dave.flink.chapter5.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SmokeLevelSource implements SourceFunction<SmokeLevel> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SmokeLevel> ctx) throws Exception {

        Random random = new Random();

        while(running){

            if (random.nextGaussian() > 0.8){
                ctx.collect(SmokeLevel.HIGH);
            }else {
                ctx.collect(SmokeLevel.LOW);
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}
