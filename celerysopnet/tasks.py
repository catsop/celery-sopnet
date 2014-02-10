from __future__ import absolute_import

from celerysopnet.celery import app

@app.task
def SliceGuarantorTask():
    """
    Calls SliceGuarantor for a certain block.
    """
    return "Called slice guarantor (dummy)"

@app.task
def SegmentGuarantorTask():
    """
    Calls SegmentGuarantor for a certain block.
    """
    return "Called segment guarantor (dummy)"

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
