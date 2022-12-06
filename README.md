# Optimal-Schedular
<<<<<<< HEAD
Apache Storm Scheduler that uses supervisor's Computational Power and bandwidth to schedule different component (Spout and Bolt).

To compile the scheduler without storm dependencies:

mvn clean

mvn assembly:assembly

To install the new scheduler in the installed Storm (for Windows) release:

cp target/autoscale-2.0.0-SNAPSHOT-jar-with-dependencies.jar %STORM%/lib

To install the new scheduler in the installed Storm (for Unix systems) release:

cp target/autoscale-2.0.0-SNAPSHOT-jar-with-dependencies.jar $STORM/lib
=======
Apache Storm Scheduler that uses supervisor's Computational Power and bandwidth to Schedule different component (Spout and Bolt).
>>>>>>> c7938f61872ac6be7b7be5841fb0f3036608d22f
