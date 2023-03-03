package storm.optimal.schedular;

import java.util.HashMap;
import java.util.Map;
import java.io.*;
import storm.optimal.Optimiser;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.utils.Utils;

import java.lang.reflect.Method;
import java.util.*;
import java.util.logging.Logger;
import org.apache.storm.scheduler.*;
import org.apache.storm.generated.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;

public class OptimalScheduler implements IScheduler {
//    private static final Logger LOG = LoggerFactory.getLogger(CustomScheduler.class);
    // private static Logger logger = Logger.getLogger("CustomScheduler");
    // logger.info     => for loging info
    // logger.severe   => for error (catched)
    // logger.fine     => mostly used (checked conditions) 
    @SuppressWarnings("rawtypes")

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
    // logger.info("Starting Custome Schedular.... (prepare function)");
    }


    // TODO
    // -> comeup with a map manualy 
    // -> feed it to the schedular 
    // -> schedule the executors to the slots according to the mapping


    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
    // logger.info("Rerunning scheduling...");
    // Optimiser opt=new optimiser(topology, cluster);
    // map<WorkerSlot, ArrayList<ExecutorDetails>> mapping=opt.optimises();
    scheduleHelper(topologies, cluster);
    // logger.info("Scheduling done...");
    }

    
    private void scheduleHelper(Topologies topologies, Cluster cluster)
    {
        Optimiser optimiser=new Optimiser(topologies, cluster);
        optimiser.getOptimalSchedule(null);
        // for (TopologyDetails topologyDetails :
        // cluster.needsSchedulingTopologies(topologies)) {
        //     StormTopology stormTopology = topologyDetails.getTopology();
        //     String topologyID = topologyDetails.getId();
        //     logger.info("Topology ID: "+topologyID);
        //     // Get components from topology
        //     Map<String, Bolt> bolts = stormTopology.get_bolts();
        //     Map<String, SpoutSpec> spouts = stormTopology.get_spouts();
        //     // Get a map of component to executors
        //     Map<String, List<ExecutorDetails>> executorsByComponent =
        //     cluster.getNeedsSchedulingComponentToExecutors(
        //     topologyDetails
        //     );
        //     // Get a map of type to components
        //     Map<String, ArrayList<String>> componentsByType = new
        //     HashMap<String, ArrayList<String>>();
        //     populateComponentsByType(componentsByType, bolts);
        //     logger.info("ComponentsByType");
        //     for(String i:componentsByType.keySet())
        //         logger.info(i+" "+" "+componentsByType.get(i).size()+componentsByType.get(i).toString());
        //     populateComponentsByType(componentsByType, spouts);
        //     for(String i:componentsByType.keySet())
        //         logger.info(i+" "+" "+componentsByType.get(i).size()+componentsByType.get(i).toString());
        //     populateComponentsByTypeWithStormInternals(componentsByType,
        //     executorsByComponent.keySet());
        //     for(String i:componentsByType.keySet())
        //         logger.info(i+" "+" "+componentsByType.get(i).size()+componentsByType.get(i).toString());

        //     // Get a map of type to executors
        //     Map<String, ArrayList<ExecutorDetails>>
        //     executorsToBeScheduledByType = getExecutorsToBeScheduledByType(
        //     cluster, topologyDetails, componentsByType
        //     );
        //     logger.info("Executors To Be Scheduled By Type.");
        //     for(String i:executorsToBeScheduledByType.keySet())
        //         logger.info(i+" "+" "+executorsToBeScheduledByType.get(i).size()+executorsToBeScheduledByType.get(i).toString());

        //     // Initialise a map of slot -> executors
        //     Map<WorkerSlot, ArrayList<ExecutorDetails>>
        //     componentExecutorsToSlotsMap = (
        //     new HashMap<WorkerSlot, ArrayList<ExecutorDetails>>()
        //     );

        //     // Time to match everything up!
        //     logger.info("Matching Started");
        //     for (Map.Entry<String, ArrayList<ExecutorDetails>> entry :
        //     executorsToBeScheduledByType.entrySet()) {
        //         String type = entry.getKey();
        //         ArrayList<ExecutorDetails> executorsForType =
        //         entry.getValue();
        //         ArrayList<SupervisorDetails> supervisorsForType =
        //         supervisorsByType.get(type);
        //         ArrayList<String> componentsForType =
        //         componentsByType.get(type);
        //         try {
        //             populateComponentExecutorsToSlotsMap(
        //             componentExecutorsToSlotsMap,
        //             cluster, topologyDetails, supervisorsForType,
        //             executorsForType, componentsForType, type
        //             );
        //         } catch (Exception e) {
        //             logger.info("Exception in populateComponentExecutorsToSlotsMap.");
        //             e.printStackTrace();
        //             // Cut this scheduling short to avoid partial scheduling.
        //             return;
        //         }
        //     }
        //     // Do the actual assigning
        //     // We do this as a separate step to only perform any assigning if there have been no issues so far.
        //     // That's aimed at avoiding partial scheduling from occurring, with some components already scheduled
        //     // and alive, while others cannot be scheduled.
        //     logger.info("Started Actual Assignment.");
        //     for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> entry :
        //     componentExecutorsToSlotsMap.entrySet()) {
        //         WorkerSlot slotToAssign = entry.getKey();
        //         ArrayList<ExecutorDetails> executorsToAssign =
        //         entry.getValue();
        //         cluster.assign(slotToAssign, topologyID,
        //         executorsToAssign);
        //         logger.info("SLOT ASSIGNED: "+slotToAssign.getNodeId()+" "+executorsToAssign.size()+" "+executorsToAssign.toString());
        //     }
        //     // If we've reached this far, then scheduling must have been successful
        //     cluster.setStatus(topologyID, "CUSTOM SUCCESSFUL");
        // }
    
    }
    
}