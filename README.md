# Scheduler architectures and mechanisms characterization

This branch contains code for the paper "ExDe: Design space exploration of scheduler architectures and mechanisms for serverless data-processing". All the code necessary to reproduce and modify the work is in the `opendc-storage/opendc-distributed-cache` folder. The schedulers and mechanisms that we characterize are in `opendc-storage/opendc-distributed-cache/src/main/kotlin/org/opendc/storage/cache/schedulers`.

The simulator uses Clikt (https://ajalt.github.io/clikt/) for the CLI. The input options can be found in the source file `opendc-storage/opendc-distributed-cache/src/main/kotlin/org/opendc/storage/cache/DistCache.kt`.

## Building the simulator

Building the simulator first requires building the support libraries. Do this using `./gradlew :opendc-experiments:opendc-experiments-capelin:assembleDist`. We just use this to build the support libraries required by the simulator.

Then, build the simulator using `./gradlew :opendc-storage:opendc-distributed-cache:fatJar`. This will produce an executable jar at `opendc-storage/opendc-distributed-cache/build/libs/opendc-distributed-cache-3.0-SNAPSHOT.jar`.

Run the jar using ` java -jar opendc-storage/opendc-distributed-cache/build/libs/opendc-distributed-cache-3.0-SNAPSHOT.jar`. This will produce an error about missing input files. The input files can be downloaded from Zenodo (https://doi.org/10.5281/zenodo.7829150). Check `opendc-storage/opendc-distributed-cache/src/main/kotlin/org/opendc/storage/cache/DistCache.kt` for CLI options.


