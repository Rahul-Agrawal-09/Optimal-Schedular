package storm.metadata;

import java.util.HashMap;
import java.util.Map;
import java.io.*;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.utils.Utils;
import org.apache.storm.metric.StormMetricsRegistry;

import java.lang.reflect.Method;
import java.util.*;
import java.util.logging.Logger;
import org.apache.storm.scheduler.*;
import org.apache.storm.generated.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;

public class MetaDataScheduler implements IScheduler {
//    private static final Logger LOG = LoggerFactory.getLogger(CustomScheduler.class);
    private static Logger logger = Logger.getLogger("CustomScheduler");
    // logger.info     => for loging info
    // logger.severe   => for error (catched)
    // logger.fine     => mostly used (checked conditions) 
    @SuppressWarnings("rawtypes")

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map<String, Object> conf, StormMetricsRegistry metricsRegistry) {
    logger.info("Starting Custome Schedular.... (prepare function)");
    }

    @Override
    public Map<String, Map<String, Double>> config() {
        return Collections.emptyMap();
    }

    // private Map<String, Number> getUserConf() {
    // Map<String, Number> ret = (Map<String, Number>)_conf.get(Config.MULTITENANT_SCHEDULER_USER_POOLS);
    // if (ret == null) {
    //     ret = new HashMap<>();
    // } else {
    //     ret = new HashMap<>(ret);
    // }

    // Map fromFile = Utils.findAndReadConfigFile("multitenant-scheduler.yaml", false);
    // Map<String, Number> tmp = (Map<String, Number>)fromFile.get(Config.MULTITENANT_SCHEDULER_USER_POOLS);
    // if (tmp != null) {
    //     ret.putAll(tmp);
    // }
    // return ret;
    // }


    // TODO
    // -> comeup with a map manualy 
    // -> feed it to the schedular 
    // -> schedule the executors to the slots according to the mapping


    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
    logger.info("Rerunning scheduling...");
    // Optimiser opt=new optimiser(topology, cluster);
    // map<WorkerSlot, ArrayList<ExecutorDetails>> mapping=opt.optimises();
    typeAwareSchedule(topologies, cluster);
    logger.info("Scheduling done...");
    }


    private Map<String, ArrayList<SupervisorDetails>> getSupervisorsByType(Collection<SupervisorDetails> supervisorDetails){
        // A map of type -> supervisors, to help with scheduling of components with specific types
        Map<String, ArrayList<SupervisorDetails>> supervisorsByType = new
        HashMap<String, ArrayList<SupervisorDetails>>();
        logger.info("Calculating supervisor by type. (get Supervisor By type)");
        for (SupervisorDetails supervisor : supervisorDetails) {
            @SuppressWarnings("unchecked")
            Map<String, String> metadata = (Map<String, String>)
            supervisor.getSchedulerMeta();
            String types;
            if (metadata == null) {
            types = "unnamed";
            } 
            else {
                types = metadata.get("name");
                logger.info("We have extracted type: "+types);
                if (types == null) {
                    types = "unnamed";
                }
            }
            // If the supervisor has types attached to it, handle it by populating the supervisorsByType map.
            // Loop through each of the types to handle individually
            for (String type : types.split(",")) {
                type = type.trim();
                if (supervisorsByType.containsKey(type)) {
                    // If we've already seen this type, then just add the supervisor to the existing ArrayList.
                    supervisorsByType.get(type).add(supervisor);
                } 
                else {
                    // If this type is new, then create a new ArrayList<SupervisorDetails>,
                    // add the current supervisor, and populate the map'stype entry with it.
                    ArrayList<SupervisorDetails> newSupervisorList = new
                    ArrayList<SupervisorDetails>();
                    newSupervisorList.add(supervisor);
                    supervisorsByType.put(type, newSupervisorList);
                }
            }
        }
        logger.info("result of getSupervisorBy type");
        for(String i:supervisorsByType.keySet())
            logger.info(i+" "+supervisorsByType.get(i).size());
        return supervisorsByType;
    }


    private <T> void populateComponentsByType(
    Map<String, ArrayList<String>> componentsByType,
    Map<String, T> components
    ) {
        // Type T can be either Bolt or SpoutSpec, so that this logic can be reused for both component types
        JSONParser parser = new JSONParser();
        for (Map.Entry<String, T> componentEntry : components.entrySet()) {
            JSONObject conf = null;
            String componentID = componentEntry.getKey();
            T component = componentEntry.getValue();
            try {
                // Get the component's conf irrespective of its type (via java reflection)
                Method getCommonComponentMethod =
                component.getClass().getMethod("get_common");
                ComponentCommon commonComponent = (ComponentCommon)
                getCommonComponentMethod.invoke(component);
                conf = (JSONObject)
                parser.parse(commonComponent.get_json_conf());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            String types;
            // If there's no config, use a fake type to group all untypeged components
            if (conf == null) {
                types = "unnamed";
            } else {
                types = (String) conf.get("name");
                // If there are no types, use a fake type to group all untypeged components
                if (types == null) {
                    types = "unnamed";
                }
            }
            // If the component has types attached to it, handle it by populating the componentsByType map.
            // Loop through each of the types to handle individually
            for (String type : types.split(",")) {
                type = type.trim();
                if (componentsByType.containsKey(type)) {
                    // If we've already seen this type, then just add the component to the existing ArrayList.
                    componentsByType.get(type).add(componentID);
                } else {
                    // If this type is new, then create a new ArrayList,
                    // add the current component, and populate the map's type entry with it.
                    ArrayList<String> newComponentList = new
                    ArrayList<String>();
                    newComponentList.add(componentID);
                    componentsByType.put(type, newComponentList);
                }
            }
        }
    }


    private void populateComponentsByTypeWithStormInternals(
    Map<String, ArrayList<String>> componentsByType,
    Set<String> components
    ) {
        // Storm uses some internal components, like __acker.
        // These components are topology-agnostic and are therefore not accessible through a StormTopology object.
        // While a bit hacky, this is a way to make sure that we schedule those components along with our topology ones:
        // we treat these internal components as regular untypeged components and add them to the componentsByType map.
        for (String componentID : components) {
            if (componentID.startsWith("__")) {
                if (componentsByType.containsKey("unnamed")) {
                    // If we've already seen untypeged components, then just add the component to the existing ArrayList.
                    componentsByType.get("unnamed").add(componentID);
                } else {
                    // If this is the first untypeged component we see, then create a new ArrayList,
                    // add the current component, and populate the map's untypeged entry with it.
                    ArrayList<String> newComponentList = new
                    ArrayList<String>();
                    newComponentList.add(componentID);
                    componentsByType.put("unnamed", newComponentList);
                }
            }
        }
    }

    
    private void typeAwareSchedule(Topologies topologies, Cluster cluster)
    {
        logger.info("Scheduling the topology. (typeAwareScheduler)");
        Collection<SupervisorDetails> supervisorDetails =
        cluster.getSupervisors().values();
        // Get the lists of typed and unreserved supervisors.
        logger.info("Got supervisor details."+supervisorDetails.toString());
        Map<String, ArrayList<SupervisorDetails>> supervisorsByType =
        getSupervisorsByType(supervisorDetails);
        logger.info("Got supervisor by type. (typeAwareScheduler)");
        for (TopologyDetails topologyDetails :
        cluster.needsSchedulingTopologies()) {
            StormTopology stormTopology = topologyDetails.getTopology();
            String topologyID = topologyDetails.getId();
            logger.info("Topology ID: "+topologyID);
            // Get components from topology
            Map<String, Bolt> bolts = stormTopology.get_bolts();
            Map<String, SpoutSpec> spouts = stormTopology.get_spouts();
            // Get a map of component to executors
            Map<String, List<ExecutorDetails>> executorsByComponent =
            cluster.getNeedsSchedulingComponentToExecutors(
            topologyDetails
            );
            // Get a map of type to components
            Map<String, ArrayList<String>> componentsByType = new
            HashMap<String, ArrayList<String>>();
            populateComponentsByType(componentsByType, bolts);
            logger.info("ComponentsByType");
            for(String i:componentsByType.keySet())
                logger.info(i+" "+" "+componentsByType.get(i).size()+componentsByType.get(i).toString());
            populateComponentsByType(componentsByType, spouts);
            for(String i:componentsByType.keySet())
                logger.info(i+" "+" "+componentsByType.get(i).size()+componentsByType.get(i).toString());
            populateComponentsByTypeWithStormInternals(componentsByType,
            executorsByComponent.keySet());
            for(String i:componentsByType.keySet())
                logger.info(i+" "+" "+componentsByType.get(i).size()+componentsByType.get(i).toString());

            // Get a map of type to executors
            Map<String, ArrayList<ExecutorDetails>>
            executorsToBeScheduledByType = getExecutorsToBeScheduledByType(
            cluster, topologyDetails, componentsByType
            );
            logger.info("Executors To Be Scheduled By Type.");
            for(String i:executorsToBeScheduledByType.keySet())
                logger.info(i+" "+" "+executorsToBeScheduledByType.get(i).size()+executorsToBeScheduledByType.get(i).toString());

            // Initialise a map of slot -> executors
            Map<WorkerSlot, ArrayList<ExecutorDetails>>
            componentExecutorsToSlotsMap = (
            new HashMap<WorkerSlot, ArrayList<ExecutorDetails>>()
            );

            // Time to match everything up!
            logger.info("Matching Started");
            for (Map.Entry<String, ArrayList<ExecutorDetails>> entry :
            executorsToBeScheduledByType.entrySet()) {
                String type = entry.getKey();
                ArrayList<ExecutorDetails> executorsForType =
                entry.getValue();
                ArrayList<SupervisorDetails> supervisorsForType =
                supervisorsByType.get(type);
                ArrayList<String> componentsForType =
                componentsByType.get(type);
                try {
                    populateComponentExecutorsToSlotsMap(
                    componentExecutorsToSlotsMap,
                    cluster, topologyDetails, supervisorsForType,
                    executorsForType, componentsForType, type
                    );
                } catch (Exception e) {
                    logger.info("Exception in populateComponentExecutorsToSlotsMap.");
                    e.printStackTrace();
                    // Cut this scheduling short to avoid partial scheduling.
                    return;
                }
            }
            // Do the actual assigning
            // We do this as a separate step to only perform any assigning if there have been no issues so far.
            // That's aimed at avoiding partial scheduling from occurring, with some components already scheduled
            // and alive, while others cannot be scheduled.
            logger.info("Started Actual Assignment.");
            for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> entry :
            componentExecutorsToSlotsMap.entrySet()) {
                WorkerSlot slotToAssign = entry.getKey();
                ArrayList<ExecutorDetails> executorsToAssign =
                entry.getValue();
                cluster.assign(slotToAssign, topologyID,
                executorsToAssign);
                logger.info("SLOT ASSIGNED: "+slotToAssign.getNodeId()+" "+executorsToAssign.size()+" "+executorsToAssign.toString());
            }
            // If we've reached this far, then scheduling must have been successful
            cluster.setStatus(topologyID, "CUSTOM SUCCESSFUL");
        }
    }

    private Set<ExecutorDetails> getAllAliveExecutors(Cluster cluster,
    TopologyDetails topologyDetails) {
        // Get the existing assignment of the current topology as it's live in the cluster
        SchedulerAssignment existingAssignment =
        cluster.getAssignmentById(topologyDetails.getId());
        // Return alive executors, if any, otherwise an empty set
        if (existingAssignment != null) {
            return existingAssignment.getExecutors();
        } else {
            return new HashSet<ExecutorDetails>();
        }
    }

    private Map<String, ArrayList<ExecutorDetails>>
    getExecutorsToBeScheduledByType(
    Cluster cluster,
    TopologyDetails topologyDetails,
    Map<String, ArrayList<String>> componentsPerType
    ) {
        // Initialise the return value
        Map<String, ArrayList<ExecutorDetails>> executorsByType = new
        HashMap<String, ArrayList<ExecutorDetails>>();
        // Find which topology executors are already assigned
        Set<ExecutorDetails> aliveExecutors = getAllAliveExecutors(cluster,
        topologyDetails);
        // Get a map of component to executors for the topology that need scheduling
        Map<String, List<ExecutorDetails>> executorsByComponent =
        cluster.getNeedsSchedulingComponentToExecutors(
        topologyDetails
        );
        // Loop through componentsPerType to populate the map
        for (Map.Entry<String, ArrayList<String>> entry :
        componentsPerType.entrySet()) {
            String type = entry.getKey();
            ArrayList<String> componentIDs = entry.getValue();
            // Initialise the map entry for the current type
            ArrayList<ExecutorDetails> executorsForType = new
            ArrayList<ExecutorDetails>();
            // Loop through this type's component IDs
            for (String componentID : componentIDs) {
                // Fetch the executors for the current component ID
                List<ExecutorDetails> executorsForComponent =
                executorsByComponent.get(componentID);
                if (executorsForComponent == null) {
                    continue;
                }
                // Convert the list of executors to a set
                Set<ExecutorDetails> executorsToAssignForComponent = new
                HashSet<ExecutorDetails>(
                executorsForComponent
                );
                // Remove already assigned executors from the set of executors to assign, if any
                executorsToAssignForComponent.removeAll(aliveExecutors);
                // Add the component's waiting to be assigned executors to the current type executors
                executorsForType.addAll(executorsToAssignForComponent);
            }
            // Populate the map of executors by type after looping through all of the type's components,
            // if there are any executors to be scheduled
            if (!executorsForType.isEmpty()) {
                executorsByType.put(type, executorsForType);
            }
        }
        return executorsByType;
    }

    private void handleFailedScheduling(
    Cluster cluster,
    TopologyDetails topologyDetails,
    String message
    ) throws Exception {
        // This is the prefix of the message displayed on Storm's UI for any unsuccessful scheduling
        String unsuccessfulSchedulingMessage = "SCHEDULING FAILED: ";
        cluster.setStatus(topologyDetails.getId(),
        unsuccessfulSchedulingMessage + message);
        throw new Exception(message);
    }


    private Set<WorkerSlot> getAllAliveSlots(Cluster cluster,
    TopologyDetails topologyDetails) {
        // Get the existing assignment of the current topology as it's live in the cluster
        SchedulerAssignment existingAssignment =
        cluster.getAssignmentById(topologyDetails.getId());
        // Return alive slots, if any, otherwise an empty set
        if (existingAssignment != null) {
            return existingAssignment.getSlots();
        } else {
            return new HashSet<WorkerSlot>();
        }
    }

    private List<WorkerSlot> getAllSlotsToAssign(
    Cluster cluster,
    TopologyDetails topologyDetails,
    List<SupervisorDetails> supervisors,
    List<String> componentsForType,
    String type
    ) throws Exception {
        String topologyID = topologyDetails.getId();
        // Collect the available slots of each of the supervisors we were given in a list
        List<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
        for (SupervisorDetails supervisor : supervisors) {
            availableSlots.addAll(cluster.getAvailableSlots(supervisor));
        }
        if (availableSlots.isEmpty()) {
            // This is bad, we have supervisors and executors to assign, but no available slots!
            String message = String.format(
            "No slots are available for assigning executors for type %s (components: %s)",
            type, componentsForType
            );
            handleFailedScheduling(cluster, topologyDetails, message);
        }
        Set<WorkerSlot> aliveSlots = getAllAliveSlots(cluster,
        topologyDetails);
        int numAvailableSlots = availableSlots.size();
        int numSlotsNeeded = topologyDetails.getNumWorkers() -
        aliveSlots.size();
        // We want to check that we have enough available slots
        // based on the topology's number of workers and already assigned slots.
        if (numAvailableSlots < numSlotsNeeded) {
            // This is bad, we don't have enough slots to assign to!
            String message = String.format(
            "Not enough slots available for assigning executors for type %s (components: %s). "
            + "Need %s slots to schedule but found only %s",
            type, componentsForType, numSlotsNeeded,
            numAvailableSlots
            );
            handleFailedScheduling(cluster, topologyDetails, message);
        }
        // Now we can use only as many slots as are required.
        return availableSlots.subList(0, numSlotsNeeded);
    }


    private Map<WorkerSlot, ArrayList<ExecutorDetails>>
    getAllExecutorsBySlot(
    List<WorkerSlot> slots,
    List<ExecutorDetails> executors
    ) {
        Map<WorkerSlot, ArrayList<ExecutorDetails>> assignments = new
        HashMap<WorkerSlot, ArrayList<ExecutorDetails>>();
        int numberOfSlots = slots.size();
        // We want to split the executors as evenly as possible, across each slot available,
        // so we assign each executor to a slot via round robin
        for (int i = 0; i < executors.size(); i++) {
            WorkerSlot slotToAssign = slots.get(i % numberOfSlots);
            ExecutorDetails executorToAssign = executors.get(i);
            if (assignments.containsKey(slotToAssign)) {
                // If we've already seen this slot, then just add the executor to the existing ArrayList.
                assignments.get(slotToAssign).add(executorToAssign);
            } else {
                // If this slot is new, then create a new ArrayList,
                // add the current executor, and populate the map's slot entry with it.
                ArrayList<ExecutorDetails> newExecutorList = new
                ArrayList<ExecutorDetails>();
                newExecutorList.add(executorToAssign);
                assignments.put(slotToAssign, newExecutorList);
            }
        }
        return assignments;
    }

    private void populateComponentExecutorsToSlotsMap(
    Map<WorkerSlot, ArrayList<ExecutorDetails>>
    componentExecutorsToSlotsMap,
    Cluster cluster,
    TopologyDetails topologyDetails,
    List<SupervisorDetails> supervisors,
    List<ExecutorDetails> executors,
    List<String> componentsForType,
    String type
    ) throws Exception {
        String topologyID = topologyDetails.getId();
        if (supervisors == null) {
        // This is bad, we don't have any supervisors but have executors to assign!
        String message = String.format(
        "No supervisors given for executors %s of topology %s and type %s (components: %s)",
        executors, topologyID, type, componentsForType
        );
        handleFailedScheduling(cluster, topologyDetails, message);
        }
        List<WorkerSlot> slotsToAssign = getAllSlotsToAssign(
        cluster, topologyDetails, supervisors, componentsForType,
        type
        );
        // Divide the executors evenly across the slots and get a map of slot to executors
        Map<WorkerSlot, ArrayList<ExecutorDetails>> executorsBySlot =
        getAllExecutorsBySlot(
        slotsToAssign, executors
        );
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> entry :
        executorsBySlot.entrySet()) {
            WorkerSlot slotToAssign = entry.getKey();
            ArrayList<ExecutorDetails> executorsToAssign =
            entry.getValue();
            // Assign the topology's executors to slots in the cluster's supervisors
            componentExecutorsToSlotsMap.put(slotToAssign, executorsToAssign);
        }
    }
}

