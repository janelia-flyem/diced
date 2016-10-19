# libdvid function for normal activity and activation
# ?! class for instance (takes node service info)

import numpy
from libdvid import DVIDNodeService

class DVIDVolume:
    uint8 = numpy.uint8
    uint16 = numpy.uint16
    uint32 = numpy.uint32
    uint64 = numpy.uint64
    def __init__(self, servername, uuid, instancename, elementsize=uint8, blocksize=64):
        # ?! will create instance if not available -- maybe make find only and have a seperate factory method to create then you connect pre-existing
        self.ns = DVIDNodeService(servername, uuid)
        self.instancename = instancename
        self.elementsize = elementsize
        if elementsize==self.uint8:
            self.ns.create_grayscale8(instancename, blocksize=blocksize)
        else:
            self.ns.create_labelblk(instancename, blocksize=blocksize)

    def get_extents(self):
        # return extents (z,y,x)
        val = self.ns.get_typeinfo(self.instancename)
        xs,ys,zs = val["Extended"]["MinPoint"]
        xf,yf,zf = val["Extended"]["MaxPoint"]
        return (slice(zs,zf+1), slice(ys,yf+1), slice(xs,xf+1))


    def __getitem__(self, index):
        if len(index) != 3:
            raise Exception("Not 3D")

        z,y,x = index

        if self.elementsize == self.uint8:
            data = self.ns.get_gray3D(self.instancename, (z.stop-z.start, y.stop-y.start, x.stop-x.start), (z.start, y.start, x.start))
        else:
            data = self.ns.get_labels3D(self.instancename, (z.stop-z.start, y.stop-y.start, x.stop-x.start), (z.start, y.start, x.start))
            data.astype(self.elementsize)

        return data

    def __setitem__(self, index, val):
        if len(index) != 3:
            raise Exception("Not 3D")
        z,y,x = index

        if self.elementsize == self.uint8:
            self.ns.put_gray3D(self.instancename, val, (z.start, y.start, x.start))
        else:
            val = val.astype(self.uint64)
            self.ns.put_labels3D(self.instancename, val, (z.start, y.start, x.start))
        
        # ?! handle non-block align and save data as uint8 or uint64

        return

