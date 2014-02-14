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
    pass

class SegmentGuarantorParameters:
    """
    This is a mock-up of pysopnet's SegmentGuarantorParameters class.
    """
    pass

class ProjectConfiguration:
    """
    This is a mock-up of pysopnet's ProjectConfiguration class.
    """
    pass

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
