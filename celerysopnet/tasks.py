from __future__ import absolute_import

from celery import chord

from celerysopnet.celery import app

import pysopnet as ps

@app.task
def SliceGuarantorTask(config, x, y, z, log_level=None):
    """
    Calls SliceGuarantor for a certain block. This task cannot fail (as long as
    there is enough space for the results).
    """
    x = int(x)
    y = int(y)
    z = int(z)
    location = ps.point3(x, y, z)
    if log_level:
        ps.setLogLevel(log_level)
    params = ps.SliceGuarantorParameters()
    ps.SliceGuarantor().fill(location, params, config)
    return "Created slice for (%s, %s, %s)" % (x, y, z)

@app.task
def SegmentGuarantorTask(config, x, y, z, fulfill_preconditions=True, log_level=None):
    """
    Calls SegmentGuarantor for a certain block. If Sopnet returns with a
    non-empty list, required slices are missing for the requested block. In this
    case, this task will create a group of slice guarantor tasks for the missing
    slices (if fulfill_preconditions is set). The task re-queues itself after
    them. It thereby makes sure all required slices are available.
    """
    x = int(x)
    y = int(y)
    z = int(z)
    # Ask Sopnet for requested segment
    location = ps.point3(x, y, z)
    if log_level:
        ps.setLogLevel(log_level)
    params = ps.SegmentGuarantorParameters()
    required_slices = ps.SegmentGuarantor().fill(location, params, config)

    # Fulfill preconditions, if any, before creating the segment
    if fulfill_preconditions and required_slices:
        # Create slice guarantor tasks for required blocks
        preconditions = [SliceGuarantorTask.s(config, rs.x, rs.y, rs.z) \
              for rs in required_slices]
        # Run a celery chain that re-executes the segment guarantor request
        # after the preconditions are met.
        callback = SegmentGuarantorTask.si(config, x, y, z)
        result = chord(preconditions)(callback)
        return "Queued %s slice guarantor tasks for positions: %s Chain ID: %s" \
                % (len(required_slices), ", ".join(map(str, required_slices)), result.task_id)
    elif required_slices:
        return "Preconditions not met for segments for block (%s, %s, %s)" % (x, y, z)
    else:
        return "Created segments for block (%s, %s, %s)" % (x, y, z)

@app.task
def SolutionGuarantorTask(config, x, y, z, fulfill_preconditions=True, log_level=None):
    """
    Calls SolutionGuarantor for a certain core.
    """
    x = int(x)
    y = int(y)
    z = int(z)
    # Ask Sopnet for requested solution
    location = ps.point3(x, y, z)
    if log_level:
        ps.setLogLevel(log_level)
    params = ps.SolutionGuarantorParameters()
    required_segments = ps.SolutionGuarantor().fill(location, params, config)

    # Fulfill preconditions, if any, before solving the core
    if fulfill_preconditions and required_segments:
        # Create slice guarantor tasks for required slices
        preconditions = [SegmentGuarantorTask.s(config, rs.x, rs.y, rs.z) \
              for rs in required_segments]
        # Run a celery chain that re-executes the solution guarantor request
        # after the preconditions are met.
        callback = SolutionGuarantorTask.si(config, x, y, z)
        result = chord(preconditions)(callback)
        return "Queued %s segment guarantor tasks for positions %s Chain ID: %s " \
                % (len(required_segments), ", ".join(map(str, required_segments)), result.task_id)
    elif required_segments:
        return "Preconditions not met for solution for core (%s, %s, %s)" % (x, y, z)
    else:
        return "Created solution for core (%s, %s, %s)" % (x, y, z)

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
