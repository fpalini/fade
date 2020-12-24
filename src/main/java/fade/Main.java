package fade;

import fade.exception.AggregationStrategyException;
import fade.simulation.Simulation;
import fade.util.Strategy;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class Main {

    static Logger logger = Logger.getLogger(Main.class);


    public static void main(String[] args) throws ConfigurationException, ClassNotFoundException, IOException, AggregationStrategyException {
        String conf_file_path = args.length > 0 ? args[0] : "fade.conf";

        Configuration conf_file = new PropertiesConfiguration(conf_file_path);

        Iterator<String> iter = conf_file.getKeys();

        fade.util.Configuration conf = new fade.util.Configuration();

	logger.info("Program started." + " Configuration file: " + conf_file_path + " loaded.");

        String key, value;
        while(iter.hasNext()) {
            key = iter.next();
            value = conf_file.getString(key);
            conf.setString(key, value);
        }

        Fade fade = new Fade(conf);

        fade.setInput(conf_file.getString("input"));
        fade.setOutput(conf_file.getString("output"));

        switch (conf.getString("strategy")) {
            case "no_aggregation": fade.setStrategy(Strategy.NO_AGGREGATION); break;
            case "partial_aggregation": fade.setStrategy(Strategy.PARTIAL_AGGREGATION); break;
            case "total_aggregation": fade.setStrategy(Strategy.TOTAL_AGGREGATION); break;
        }

        fade.setStatisticExtractor(Class.forName(conf_file.getString("extractor")));

        if (conf.getString("aggregator") != null)
            fade.setStatisticAggregator(Class.forName(conf_file.getString("aggregator")));

        ArrayList<String> evaluator_class_names = (ArrayList<String>) conf_file.getList("evaluator");

        for (String evaluator_name : evaluator_class_names)
            fade.addAFFunctionEvaluator(Class.forName(evaluator_name));

        String task = conf.getString("task");

        System.out.println("Configuration Loaded: " + conf_file_path);

        if (task.equals("simulation")) {
            Simulation simulation = new Simulation(fade);
            simulation.simulate();
        }
        else {// distance
            fade.compute();
        }

        fade.close();
    }
}
