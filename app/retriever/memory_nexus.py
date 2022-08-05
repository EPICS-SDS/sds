from essnexus.essnexus import File
import h5py as hp


class MemoryNexus(File):
    """
    This is a subclass of the ess-nexus File class, which takes a file-like
    object as a parameter to store the data.
    """

    def __init__(self, h5_io):
        super().__init__()

        self.h5file = hp.File(h5_io, "w")

        # add groups:
        # File entry
        self.nxentry = self.h5file.create_group("entry")
        self.nxentry.attrs["NX_class"] = "NXentry"

        # File User
        self.nxentry.create_group("user")
        self.nxentry["user"].attrs["NX_class"] = "NXuser"

        # File Instruments
        self.nxentry.create_group("instruments")
        self.nxentry["instruments"].attrs["NX_class"] = "NXinstrument"

    def copy(self, group):
        self.h5file["entry"].copy(group, self.h5file["entry"])
