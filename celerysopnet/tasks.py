from __future__ import absolute_import

from celery import chord

from celerysopnet.celery import app

import pysopnet as ps

@app.task
def SliceGuarantorTask(config, x, y, z):
    """
    Calls SliceGuarantor for a certain block. This task cannot fail (as long as
    there is enough space for the results).
    """
    ps.setLogLevel(3)
    x = int(x)
    y = int(y)
    z = int(z)
    location = ps.point3(x, y, z)
    params = ps.SliceGuarantorParameters()
    pconfig = create_project_config(config)
    ps.SliceGuarantor().fill(location, params, pconfig)
    return "Created slice for (%s, %s, %s)" % (x, y, z)

@app.task
def SegmentGuarantorTask(config, x, y, z):
    """
    Calls SegmentGuarantor for a certain block. If Sopnet returns with a
    non-empty list, slices are missing for the segment to be generated. In this
    case, this task will create a group of slice guarantor tasks for the missing
    slices. It re-queues itself after them. It thereby makes sure all required
    slices are available.
    """
    ps.setLogLevel(3)
    x = int(x)
    y = int(y)
    z = int(z)
    # Ask Sopnet for requested segment
    location = ps.point3(x, y, z)
    params = ps.SegmentGuarantorParameters()
    pconfig = create_project_config(config)
    required_slices = ps.SegmentGuarantor().fill(location, params, pconfig)

    # Fulfill preconditions, if any, before creating the segment
    if required_slices:
        # Create slice guarantor tasks for required slices
        preconditions = [SliceGuarantorTask.s(config, rs.x, rs.y, rs.z) \
              for rs in required_slices]
        # Create a list of the precdonditions' coordinats
        precondition_coords = ["(%s, %s, %s)" % (rs.x, rs.y, rs.z) \
              for rs in required_slices]
        # Run a celery chain that re-executes the slice guarantor request after
        # the preconditions are met.
        callback = SegmentGuarantorTask.si(config, x, y ,z)
        result = chord(preconditions)(callback)
        return "Queued slice guarantor tasks for positions: %s Chain ID: %s" \
                % (", ".join(precondition_coords), result.task_id)
    else:
        return "Created segment for (%s, %s, %s)" % (x, y, z)

@app.task
def SolutionGuarantorTask(config, x, y, z):
    """
    Calls SolutionGuarantor for a certain core.
    """
    ps.setLogLevel(3)
    x = int(x)
    y = int(y)
    z = int(z)
    # Ask Sopnet for requested solution
    location = ps.point3(x, y, z)
    params = ps.SolutionGuarantorParameters()
    pconfig = create_project_config(config)
    required_segments = ps.SolutionGuarantor().fill(location, params, pconfig)

    # Fulfill preconditions, if any, before creating the segment
    if required_segments:
        # Create slice guarantor tasks for required slices
        preconditions = [SegmentGuarantorTask.s(config, rs.x, rs.y, rs.z) \
              for rs in required_segments]
        # Run a celery chain that re-executes the slice guarantor request after
        # the preconditions are met.
        callback = SolutionGuarantorTask.si(config, x, y, z)
        result = chord(preconditions)(callback)
        return "Queued %s new segment guarantor tasks and a new solution " \
                "guarantor task: %s" % (len(preconditions), result.task_id)
    else:
        return "Created solution"

@app.task
def SolveSubvolumeTask():
    """
    Calls SolutionGuarantorTask for all cores in a subvolume.
    """
    return "Called solution guarantor task for all cores"

@app.task
def TraceNeuronTask():
    """
    Actively follows the cores of a neuron around a requested point.
    """
    return "Finished tracing neuron"

def create_project_config(config):
    """
    This takes any dictionary and creates a real project configuration. This
    dictinary can have the following keys and values:

        backend_type = [local|django]
        catmaid_host = [Host:port of CATMAID]
        catmaid_stack_id = [ID of the CATMAID stack in question]
        catmaid_project_id = [ID of the CATMAID project in question]
        block_size = [3 element array for the block size]
        volume_size = [3 element array for the volume size]
        core_size = [3 element array for the core size]
    """
    pc = ps.ProjectConfiguration()

    backend_type = config.get('backend_type', 'local')
    if backend_type == 'django':
        pc.setBackendType(ps.BackendType.Django)
    else:
        pc.setBackendType(ps.BackendType.Local)

    catmaid_host = config.get('catmaid_host')
    if catmaid_host:
        pc.setCatmaidHost(catmaid_host)

    catmaid_raw_stack_id = config.get('catmaid_raw_stack_id')
    if catmaid_raw_stack_id:
        pc.setCatmaidRawStackId(int(catmaid_raw_stack_id))

    catmaid_mem_stack_id = config.get('catmaid_membrane_stack_id')
    if catmaid_mem_stack_id:
        pc.setCatmaidMembraneStackId(int(catmaid_mem_stack_id))

    catmaid_project_id = config.get('catmaid_project_id')
    if catmaid_project_id:
        pc.setCatmaidProjectId(int(catmaid_project_id))

    block_size = config.get('block_size')
    if block_size:
        pc.setBlockSize(ps.point3(block_size[0], block_size[1], block_size[2]))

    volume_size = config.get('volume_size')
    if volume_size:
        pc.setVolumeSize(ps.point3(volume_size[0], volume_size[1], volume_size[2]))

    core_size = config.get('core_size')
    if core_size:
        pc.setCoreSize(ps.point3(core_size[0], core_size[1], core_size[2]))

    return pc
