package storm.optimal;

import java.util.HashMap;
import java.util.Map;
import java.io.*;
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

public class Optimiser {
    // private static Logger logger = Logger.getLogger("Optimiser");
    Map<String, SupervisorDetails> supervisorsByName;
    private Topologies topologies;
    private Cluster cluster;
    
    public Optimiser(Topologies topologies, Cluster cluster){
        this.topologies=topologies;
        this.cluster=cluster;
        Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();
        
        // Get the map of name and supervisors.
        // logger.info("Got supervisor details."+supervisorDetails.toString());
        this.supervisorsByName = getSupervisorsByName(supervisorDetails);
        // logger.info("Got supervisor by type. (getOptimalManualSchedule)");
    }

    public void getOptimalSchedule(Map<WorkerSlot,ArrayList<ExecutorDetails>> optimalMapping){
        getOptimalManualSchedule(optimalMapping);
    }

    private void getOptimalManualSchedule(Map<WorkerSlot,ArrayList<ExecutorDetails>> optimalMapping){
        // logger.info("Scheduling the topology. (getOptimalManualSchedule)");
        
        for (TopologyDetails topologyDetails : cluster.needsSchedulingTopologies(topologies)) {
            StormTopology stormTopology = topologyDetails.getTopology();
            String topologyID = topologyDetails.getId();
            // logger.info("Topology ID: "+topologyID);
            // Get components from topology
            Map<String, Bolt> bolts = stormTopology.get_bolts();
            Map<String, SpoutSpec> spouts = stormTopology.get_spouts();
            // Get a map of component to executors
            Map<String, List<ExecutorDetails>> executorsByComponent =
            cluster.getNeedsSchedulingComponentToExecutors(topologyDetails);
            // Get a map of type to components
            Map<String, ArrayList<String>> componentsByName = new HashMap<String, ArrayList<String>>();
            
            // populateComponentsByName(componentsByName, bolts);
            // logger.info("componentsByName");
            // for(String i:componentsByName.keySet())
            //     logger.info(i+" "+" "+componentsByName.get(i).size()+componentsByName.get(i).toString());
            
            // populateComponentsByName(componentsByName, spouts);
            // for(String i:componentsByName.keySet())
            //     logger.info(i+" "+" "+componentsByName.get(i).size()+componentsByName.get(i).toString());
            
            populateComponentsByName(componentsByName, executorsByComponent);
            // for(String i:componentsByName.keySet())
            //     logger.info(i+" "+" "+componentsByName.get(i).size()+componentsByName.get(i).toString());

            // Get a map of type to executors
            Map<String, ArrayList<ExecutorDetails>>
            executorsToBeScheduledByName = getExecutorsToBeScheduledByName(
            cluster, topologyDetails, componentsByName
            );
            // logger.info("Executors To Be Scheduled By Type.");
            // for(String i:executorsToBeScheduledByName.keySet())
                // logger.info(i+" "+" "+executorsToBeScheduledByName.get(i).size()+executorsToBeScheduledByName.get(i).toString());

            // Initialise a map of slot -> executors
            Map<WorkerSlot, ArrayList<ExecutorDetails>>
            componentExecutorsToSlotsMap = (
            new HashMap<WorkerSlot, ArrayList<ExecutorDetails>>()
            );

            // Time to match everything up!
            // logger.info("Matching Started");

            // NO TESTING IS DONE=> TODO Handle corner cases 
            for (Map.Entry<String, ArrayList<ExecutorDetails>> entry :
            executorsToBeScheduledByName.entrySet()) {
                String type = entry.getKey();
                ArrayList<ExecutorDetails> executorsForName =
                entry.getValue();
                SupervisorDetails supervisorsForName =
                this.supervisorsByName.get(type);
                ArrayList<String> componentsForName =
                componentsByName.get(type);
                try {
                    populateComponentExecutorsToSlotsMap(
                    componentExecutorsToSlotsMap,
                    cluster, topologyDetails, supervisorsForName,
                    executorsForName, componentsForName, type
                    );
                } catch (Exception e) {
                    // logger.info("Exception in populateComponentExecutorsToSlotsMap.");
                    e.printStackTrace();
                    // Cut this scheduling short to avoid partial scheduling.
                    return;
                }
            }
            
            // Do the actual assigning
            // We do this as a separate step to only perform any assigning if there have been no issues so far.
            // That's aimed at avoiding partial scheduling from occurring, with some components already scheduled
            // and alive, while others cannot be scheduled.
            // logger.info("Started Actual Assignment.");
            for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> entry :
            componentExecutorsToSlotsMap.entrySet()) {
                WorkerSlot slotToAssign = entry.getKey();
                ArrayList<ExecutorDetails> executorsToAssign =
                entry.getValue();
                cluster.assign(slotToAssign, topologyID,
                executorsToAssign);
                // logger.info("SLOT ASSIGNED: "+slotToAssign.getNodeId()+" "+executorsToAssign.size()+" "+executorsToAssign.toString());
            }
            // If we've reached this far, then scheduling must have been successful
            cluster.setStatus(topologyID, "OPTIMAL SUCCESSFUL");
        }

    }


    private Map<String, SupervisorDetails> getSupervisorsByName(
        Collection<SupervisorDetails> supervisorDetails){
        // A map of type -> supervisors, to help with scheduling of components with specific types
        Map<String, SupervisorDetails> supervisorsByName = new HashMap<String, SupervisorDetails>();
        // logger.info("Calculating supervisor by Name. (get Supervisor By type)");
        Integer unnamedSupervisorCount=0;
        for (SupervisorDetails supervisor : supervisorDetails) {
            @SuppressWarnings("unchecked")
            Map<String, String> metadata = (Map<String, String>)
            supervisor.getSchedulerMeta();
            String name;
            if (metadata == null) {
                name = "unnamed"+(unnamedSupervisorCount++);
            } 
            else {
                name = metadata.get("name");
                // logger.info("We have extracted name: "+name);
                if (name == null || supervisorsByName.containsKey(name)) {
                    name = "unnamed"+(unnamedSupervisorCount++);
                }
            }
            // If duplicate names are given then convert the new supervisor to unname
            supervisorsByName.put(name, supervisor);
        }
        // logger.info("result of getSupervisorBy type");
        // for(String i:supervisorsByName.keySet())
        //     logger.info(i+" "+supervisorsByName.get(i));
        return supervisorsByName;
    }


    private void populateComponentsByName(
    Map<String, ArrayList<String>> componentsByType,Map<String, List<ExecutorDetails>> components) {
        // Manually allocating components to optimal slot
        // cs2-ubuntu=> node1
        // provienance => node2
        // Components => spout, bolt, publisher, database, __acker

        // wordGenerator, counter, aggregator, intermediateRanker, finalRanker, __acker
        componentsByType.put("node1", new ArrayList<String>());
        componentsByType.put("node2", new ArrayList<String>());
        componentsByType.get("node1").add("wordGenerator");
        componentsByType.get("node1").add("aggregator");
        componentsByType.get("node1").add("finalRanker");
        componentsByType.get("node2").add("__acker");
        componentsByType.get("node2").add("counter");
        componentsByType.get("node2").add("intermediateRanker");
    }


    // No modification done here
    private Map<String, ArrayList<ExecutorDetails>>
    getExecutorsToBeScheduledByName(Cluster cluster, TopologyDetails topologyDetails,
    Map<String, ArrayList<String>> componentsPerType) {
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


    private void populateComponentExecutorsToSlotsMap(
    Map<WorkerSlot, ArrayList<ExecutorDetails>>
    componentExecutorsToSlotsMap,
    Cluster cluster,
    TopologyDetails topologyDetails,
    SupervisorDetails supervisors,
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


    private List<WorkerSlot> getAllSlotsToAssign(
        Cluster cluster,
        TopologyDetails topologyDetails,
        SupervisorDetails supervisors,
        List<String> componentsForType,
        String type
        ) throws Exception {
            String topologyID = topologyDetails.getId();
            // Collect the available slots of each of the supervisors we were given in a list
            List<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
            availableSlots.addAll(cluster.getAvailableSlots(supervisors));
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


}