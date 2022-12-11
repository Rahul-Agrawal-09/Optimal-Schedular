# Optimal-Schedular
Apache Storm Scheduler that uses supervisor's Computational Power and bandwidth to schedule different component (Spout and Bolt).

---
## Varients
#### **scheduler-1.1.0** 
- This varient is for apache storm 1.1.0
#### **scheduler-2.4.0**
- This varient is for apache storm 2.4.0

---
## To compile the scheduler without storm dependencies:

### How to Package schedulers
```sh
$ mvn clean
$ mvn assembly:assembly 
# Generated package will be here: target/<schedulerName>-2.0.0-SNAPSHOT-jar-with-dependencies.jar 
```

### How to add schedulers in Apache Storm
```sh
# for windows
> cp target/autoscale-2.0.0-SNAPSHOT-jar-with-dependencies.jar %STORM%/lib

# for linux
$ cp target/autoscale-2.0.0-SNAPSHOT-jar-with-dependencies.jar $STORM_HOME/lib
```

### How to use schedulers in Apache Storm
Add following line in storm.yaml file
```sh
# for OptimalScheduler
storm.scheduler:
   "storm.optimal.schedular.OptimalScheduler"

# for MetaDataScheduler
storm.scheduler:
   "storm.metadata.MetaDataScheduler"
```