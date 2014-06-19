from __future__ import absolute_import

from celery import chord

from celerysopnet.celery import app

import celerysopnet.mockups as ps
#import pysopnet as ps

@app.task
def SliceGuarantorTask(config, x, y, z):
    """
    Calls SliceGuarantor for a certain block. This task cannot fail (as long as
    there is enough space for the results).
    """
    location = ps.point3(x, y, z)
    params = ps.SliceGuarantorParameters()
    result = ps.SliceGuarantor().fill(location, params, config)
    return "Created slice for (%s, %s, %s) (dummy)" % (x, y, z)

@app.task
def SegmentGuarantorTask(config, x, y, z):
    """
    Calls SegmentGuarantor for a certain block. If Sopnet returns with a
    non-empty list, slices are missing for the segment to be generated. In this
    case, this task will create a group of slice guarantor tasks for the missing
    slices. It re-queues itself after them. It thereby makes sure all required
    slices are available.
    """
    # Ask Sopnet for requested segment
    location = ps.point3(x, y, z)
    params = ps.SegmentGuarantorParameters()
    required_slices = ps.SegmentGuarantor().fill(location, params, config)

    # Fulfill preconditions, if any, before creating the segment
    if required_slices:
        # Create slice guarantor tasks for required slices
        preconditions = [SliceGuarantorTask.s(rs.x, rs.y, rs.z) \
              for rs in required_slices]
        # Run a celery chain that re-executes the slice guarantor request after
        # the preconditions are met.
        callback = SegmentGuarantorTask.si(location, params, config)
        result = chord(preconditions)(callback)
        return "Queued %s new slice guarantor tasks and new segment " \
                "guarantor task: %s (dummy)" % (len(preconditions), result.task_id)
    else:
        return "Created segment (dummy)"

@app.task
def SolutionGuarantorTask(config, x, y, z):
    """
    Calls SolutionGuarantor for a certain core.
    """
    # Ask Sopnet for requested solution
    location = ps.point3(x, y, z)
    params = ps.SolutionGuarantorParameters()
    required_segments = ps.SolutionGuarantor().fill(location, params, config)

    # Fulfill preconditions, if any, before creating the segment
    if required_segments:
        # Create slice guarantor tasks for required slices
        preconditions = [SegmentGuarantorTask.s(config, rs.x, rs.y, rs.z) \
              for rs in required_segments]
        # Run a celery chain that re-executes the slice guarantor request after
        # the preconditions are met.
        callback = SolutionGuarantorTask.si(location, params, config)
        result = chord(preconditions)(callback)
        return "Queued %s new segment guarantor tasks and a new solution " \
                "guarantor task: %s (dummy)" % (len(preconditions), result.task_id)
    else:
        return "Created solution (dummy)"

@app.task
def SolveSubvolumeTask():
    """
    Calls SolutionGuarantorTask for all cores in a subvolume.
    """
    return "Called solution guarantor task for all cores (dummy)"

@app.task
def TraceNeuronTask():
    """
    Actively follows the cores of a neuron around a requested point.
    """
    return "Finished tracing neuron (dummy)"
