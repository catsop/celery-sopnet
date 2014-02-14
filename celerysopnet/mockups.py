from random import randint

class point3:
    """
    This is a mock-up of pysopnet's point3 class.
    """
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

class SliceGuarantorParameters:
    """
    This is a mock-up of pysopnet's SliceGuarantorParameters class.
    """
    def __init__(self):
        self.max_slice_size = 0

    def getMaxSliceSize(self):
        return self.max_slice_size

    def setMaxSliceSize(self, new_size):
        self.max_slice_size = new_size

class SegmentGuarantorParameters:
    """
    This is a mock-up of pysopnet's SegmentGuarantorParameters class.
    """
    pass

class SolutionGuarantorParameters:
    """
    This is a mock-up of pysopnet's SolutionGuarantorParameters class.
    """
    pass

class ProjectConfiguration:
    """
    This is a mock-up of pysopnet's ProjectConfiguration class.
    """
    def __init__(self):
        self.block_size = point3(0, 0 ,0)
        self.django_url = ""

    def getBlockSize(self):
        return self.block_size

    def setBlockSize(self, new_size):
        self.block_size = new_size

    def getDjangoUrl(self):
        return self.django_url

    def setDjangoUrl(self, new_url):
        self.django_url = new_url

class SliceGuarantor:
    """
    This is a mock-up of pysopnet's SliceGuarantor class. It doesn't actually
    create data.
    """
    def fill(self, location, params, config):
        pass

class SegmentGuarantor:
    """
    This is a mock-up of pysopnet's SegmentGuarantor class. It doesn't actually
    create data. It returns a randomly created list of required slices.
    """
    def fill(self, location, params, config):
        required_slices = [
                point3(randint(0,10), randint(0,10), randint(0,10)) \
                for i in range(randint(0,3))]
        return required_slices

class SolutionGuarantor:
    """
    This is a mock-up of pysopnet's SolutionGuarantor class. It doesn't actually
    create data. It returns a randomly created list of required segments
    (between and including zero and three).
    """
    def fill(self, location, params, config):
        required_segments = [
                point3(randint(0,10), randint(0,10), randint(0,10)) \
                for i in range(randint(0,3))]
        return required_segments
