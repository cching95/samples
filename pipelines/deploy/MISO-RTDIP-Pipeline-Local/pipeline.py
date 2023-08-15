import sys
import os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from rtdip_sdk.pipelines.execute import PipelineJob, PipelineStep, PipelineTask, PipelineJobExecute
from rtdip_sdk.pipelines.sources import MISODailyLoadISOSource
from rtdip_sdk.pipelines.transformers import MISOToMDMTransformer
from rtdip_sdk.pipelines.destinations import SparkDeltaDestination

step_list = []

# Read step
step_list.append(PipelineStep(
    name="step_1",
    description="Read Forecast data from MISO API.",
    component=MISODailyLoadISOSource,
    component_parameters={"options": {
        "load_type": "actual",
        "date": "20230520",
    },
    },
    provide_output_to_step=["step_2", "step_4"]
))

# Transform step - Values
step_list.append(PipelineStep(
    name="step_2",
    description="Get measurement data.",
    component=MISOToMDMTransformer,
    component_parameters={
        "output_type": "usage",
    },
    depends_on_step=["step_1"],
    provide_output_to_step=["step_3"]
))

# Write step
step_list.append(PipelineStep(
    name="step_3",
    description="Write measurement data to Delta table.",
    component=SparkDeltaDestination,
    component_parameters={
        "destination": "MISO_ISO_Usage_Data",
        "options": {
            "partitionBy":"timestamp"
        },
        "mode": "overwrite"
    },
    depends_on_step=["step_2"]
))

# Transformer step - Meta information
step_list.append(PipelineStep(
    name="step_4",
    description="Get meta information.",
    component=MISOToMDMTransformer,
    component_parameters={
        "output_type": "meta",
    },
    depends_on_step=["step_1"],
    provide_output_to_step=["step_5"]
))

# Write step
step_list.append(PipelineStep(
    name="step_5",
    description="step_5",
    component=SparkDeltaDestination,
    component_parameters={
        "destination": "MISO_ISO_Meta_Data",
        "options": {},
        "mode": "overwrite"
    },
    depends_on_step=["step_4"]
))

# Tasks contain a list of steps
task = PipelineTask(
    name="test_task",
    description="test_task",
    step_list=step_list,
    batch_task=True
)

# Job containing a list of tasks
pipeline_job = PipelineJob(
    name="test_job",
    description="test_job",
    version="0.0.1",
    task_list=[task]
)

# Execute
pipeline = PipelineJobExecute(pipeline_job)

result = pipeline.run()