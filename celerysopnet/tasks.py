from __future__ import absolute_import

from celery import chord

from celerysopnet.celery import app

import pysopnet as ps

from random import randint

def call_segment_guarantor():
    """
    This is for now a mock-up which randomly returns required slices.
    """
    required_slices = [ps.point3(randint(0,10), randint(0,10), randint(0,10)) \
            for i in range(randint(0,3))]

    return required_slices

@app.task
def SliceGuarantorTask(x, y, z):
    """
    Calls SliceGuarantor for a certain block. This task cannot fail (as long as
    there is enough space for the results).
    """
    return "Created slice for (%s, %s, %s) (dummy)" % (x, y, z)

@app.task
def SegmentGuarantorTask():
    """
    Calls SegmentGuarantor for a certain block. If Sopnet returns with a
    non-empty list, slices are missing for the segment to be generated. In this
    case, this task will create a group of slice guarantor tasks for the missing
    slices. It re-queues itself after them. It thereby makes sure all required
    slices are available.
    """
    # Ask Sopnet for requested segment
    required_slices = call_segment_guarantor()

    # Fulfill preconditions, if any, before creating the segment
    if required_slices:
        # Create slice guarantor tasks for required slices
        preconditions = [SliceGuarantorTask.s(rs.x, rs.y, rs.z) \
              for rs in required_slices]
        # Run a celery chain that re-executes the slice guarantor request after
        # the preconditions are met.
        callback = SegmentGuarantorTask.si()
        result = chord(preconditions)(callback)
        return "Queued %s new slice guarantor tasks and new segment " \
                "guarantor task: %s (dummy)" % (len(preconditions), result.task_id)
    else:
        return "Created segment (dummy)"

@app.task
def SolutionGuarantorTask():
    """
    Calls SolutionGuarantor for a certain core.
    """
    return "Called solution guarantor (dummy)"

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
