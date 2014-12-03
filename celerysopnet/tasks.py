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
    ps.setLogLevel(int(config.get('loglevel', 2)))
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
    ps.setLogLevel(int(config.get('loglevel', 2)))
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
        return "Queued %s slice guarantor tasks for positions: %s Chain ID: %s" \
                % (len(required_slices), ", ".join(precondition_coords), result.task_id)
    else:
        return "Created segment for (%s, %s, %s)" % (x, y, z)

@app.task
def SolutionGuarantorTask(config, x, y, z):
    """
    Calls SolutionGuarantor for a certain core.
    """
    ps.setLogLevel(int(config.get('loglevel', 2)))
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
        # Create a list of the precdonditions' coordinats
        precondition_coords = ["(%s, %s, %s)" % (rs.x, rs.y, rs.z) \
              for rs in required_segments]
        # Run a celery chain that re-executes the slice guarantor request after
        # the preconditions are met.
        callback = SolutionGuarantorTask.si(config, x, y, z)
        result = chord(preconditions)(callback)
        return "Queued %s segment guarantor tasks for positions %s Chain ID: %s " \
                % (len(required_segments), ", ".join(precondition_coords), result.task_id)
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

        backend_type = [local|postgresql]
        catmaid_host = [Host:port of CATMAID]
        catmaid_stack_id = [ID of the CATMAID stack in question]
        catmaid_project_id = [ID of the CATMAID project in question]
        block_size = [3 element array for the block size in voxels]
        volume_size = [3 element array for the volume size in voxels]
        core_size = [3 element array for the core size in blocks]
        component_dir = [Path to connect component storage directory]
        postgresql_host = [PostgreSQL host]
        postgresql_port = [PostgreSQL port]
        postgresql_user = [PostgreSQL user]
        postgresql_password = [PostgreSQL password]
        postgresql_database = [PostgreSQL database]
    """
    pc = ps.ProjectConfiguration()

    backend_type = config.get('backend_type', 'local')
    if backend_type == 'postgresql':
        pc.setBackendType(ps.BackendType.PostgreSql)
    else:
        pc.setBackendType(ps.BackendType.Local)

    block_size = config.get('block_size')
    if block_size:
        pc.setBlockSize(ps.point3(block_size[0], block_size[1], block_size[2]))

    volume_size = config.get('volume_size')
    if volume_size:
        pc.setVolumeSize(ps.point3(volume_size[0], volume_size[1], volume_size[2]))

    core_size = config.get('core_size')
    if core_size:
        pc.setCoreSize(ps.point3(core_size[0], core_size[1], core_size[2]))

    # Set the remaining parameters that do not require special handling.
    param_mapping = [
            {'name': 'catmaid_host', 'set': pc.setCatmaidHost, 'parse': str},
            {'name': 'catmaid_raw_stack_id', 'set': pc.setCatmaidRawStackId, 'parse': int},
            {'name': 'catmaid_membrane_stack_id', 'set': pc.setCatmaidMembraneStackId, 'parse': int},
            {'name': 'catmaid_project_id', 'set': pc.setCatmaidProjectId, 'parse': int},
            {'name': 'component_dir', 'set': pc.setComponentDirectory, 'parse': str},
            {'name': 'postgresql_host', 'set': pc.setPostgreSqlHost, 'parse': str},
            {'name': 'postgresql_port', 'set': pc.setPostgreSqlPort, 'parse': str},
            {'name': 'postgresql_user', 'set': pc.setPostgreSqlUser, 'parse': str},
            {'name': 'postgresql_password', 'set': pc.setPostgreSqlPassword, 'parse': str},
            {'name': 'postgresql_database', 'set': pc.setPostgreSqlDatabase, 'parse': str}]
    for param_map in param_mapping:
        param_value = config.get(param_map['name'])
        # Only set the parameter if it is specified in config
        if param_value:
            param_map['set'](param_map['parse'](param_value))

    return pc
