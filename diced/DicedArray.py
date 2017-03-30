"""Functionality to store and retrieve data into Diced arrays.
"""

import numpy as np

class DicedArray(object):
    """Implements acces to nD arrays in Diced (__init__ internally by DicedRepo)

    Diced supports 1D, 2D, and 3D arrays for 8, 16, 32, and
    64 bits.  This data is stored using lossless compression unless
    lossy JPEG compression is chosen when the array is created.

    Data can be retrieved or stored using any index int location.
    The current extents just refers to the BBOX for the currently
    written data.

    For array data that is a non-noisy labeling (potentially as
    a result of object prediction), it is best to choose 
    a Diced label array which will support up to 64 bit data and
    whose compression is mostly independent of the size required
    for each data element.

    Note:
        Reserve JPEG compression and label types for 3D data only.


    TODO:
        Support a general ND interface.  This depends on the creation
        of this API in DVID.

        Extents is a crude way to note what data has been changed.
        Eventually, there should be a more precise way to note all the
        voxels that have been written to in the array.
    """

    MAX_REQ_SIZE = 512*512*512

    def __init__(self, name, dicedstore, locked, nodeconn, allmeta, numdims, dtype, islabel3D):
        """Internal Init.

        Init is called by DicedRepo.  It has a pointer to dicedstore
        to ensure DiceStore is active while this instance is around.

        Args:
            name (str): datatype instance
            dicedstore (DicedStore): referenced to DicedStore
            locked (str): if node locked, read only
            nodeconn (libdvid object): connection to DVID version node
            allmeta (json): metadata for instance
            numdims (int): number of dimensions for array
            dtype (ArrayDtype): array datatype
        """

        self.name = name
        self.dicedstore = dicedstore
        self.locked = locked
        self.nodeconn = nodeconn
        self.numdims = numdims
        self.dtype = dtype
        self.islabel3D = islabel3D

        # extract specific meta
        self.blocksize = allmeta["Extended"]["BlockSize"]
    
    def get_extents(self):
        """Retrieve extants for array.

        Returns:
            tuple of slices
        """

        # return extents (z,y,x)
        val = self.nodeconn.get_typeinfo(self.instancename)
        xs,ys,zs = val["Extended"]["MinPoint"]
        xf,yf,zf = val["Extended"]["MaxPoint"]
        
        if self.numdims == 3:
            return (slice(zs,zf+1), slice(ys,yf+1), slice(xs,xf+1))
        elif self.numdims == 2:
            return (slice(ys,yf+1), slice(xs,xf+1))
        elif self.numdims == 1:
            return (slice(xs,xf+1))


    def _getchunk(self, z, y, x, zsize, ysize, xsize):
        """Internal function to retrieve data.
        """
        
        data = None
        
        # interface is the same for labels and raw arrays but the function is stateless
        # and can benefit from extra compression possible in labels in some use cases
        if self.dtype = ArrayDtype.uint8:
            data = self.ns.get_array8bit(self.instancename, (zsize, ysize, xsize), (z, y, x), self.islabels3D)
        elif self.dtype = ArrayDtype.uint16:
            data = self.ns.get_array16bit(self.instancename, (zsize, ysize, xsize), (z, y, x), self.islabels3D)
        elif self.dtype = ArrayDtype.uint32:
            data = self.ns.get_array32bit(self.instancename, (zsize, ysize, xsize), (z, y, x), self.islabels3D)
        elif self.dtype = ArrayDtype.uint64:
            data = self.ns.get_array64bit(self.instancename, (zsize, ysize, xsize), (z, y, x), self.islabels3D)
        else:
            raise DicedException("Invalid datatype for array")

        return data


    def __getitem__(self, index):
        """Use index to retrieve array data.
        
        Note:
            Large requests are split into several small requests.  Data
            can be requested outside of the extents.
        """

        if len(index) > 3:
            raise DicedException("Does not support arrays with dimension greater than 3")

        z = y = x = slice(0,1)
        if len(index) == 3:
            z,y,x = index
        elif len(index) == 2:
            y, x = index
        else:
            x = index

        data = None

        # only support calls to volumes <= 512x512x512, larger calls should
        # be split into several pieces
        zsize = z.stop - z.start
        ysize = y.stop - y.start
        xsize = x.stop - x.start
        if zsize*ysize*xsize > self.MAX_REQ_SIZE: 
            data = numpy.zeros((zsize, ysize, xsize), dtype.value)
       
            # split into chunks
            zincr = zsize    
            yincr = ysize    
            xincr = xsize    
       
            while zincr*yincr*xincr > self.MAX_REQ_SIZE:
                if zincr > yincr and zincr > xincr:
                    zincr = zincr/2 + zincr % 2
                elif yincr > zincr and yincr > xincr:
                    yincr = yincr/2 + yincr % 2
                else:
                    xincr = xincr/2 + xincr % 2

            for ziter in range(0, zsize, zincr):
                for yiter in range(0, ysize, yincr):
                    for xiter in range(0, xsize, xincr):
                        zstart = ziter + z.start
                        ystart = yiter + y.start
                        xstart = xiter + x.start
                       
                        csizez = min(zsize - zstart, zincr)
                        csizey = min(ysize - ystart, yincr)
                        csizex = min(xsize - xstart, xincr)

                        tdata = self._getchunk(zstart, ystart, xstart, csizez, csizey, csizex)
                        data[ziter:ziter+csizez, yiter:yiter+csizey, xiter:xiter+csizex] = tdata
        else:
            # small call can be had in one call
            data = self._getchunk(z.start, y.start, x.start, zsize, ysize, xsize)


        return data
    
    def _setchunk(self, z, y, x, data):
        """Internal function to set data.
        """
        
        # check if block aligned and adjust extents
        xblk, yblk, zblk = self.blocksize
        zsize, ysize, xsize = data.shape
        zsizeorig, ysizeorig, xsizeorig = data.shape
        
        blockaligned = True
       
        znew -= (z%zblk)
        ynew -= (y%zblk)
        xnew -= (x%zblk)
        
        zsize += (z%zblk)
        ysize += (y%zblk)
        xsize += (x%zblk)

        if znew != z or ynew != y or xnew != x:
            blockaligned = False
       
        if zsize % zblk > 0:
            blockaligned = False
            zsize += (zblk - zsize%zblk)
        
        if ysize % yblk > 0:
            blockaligned = False
            ysize += (yblk - ysize%yblk)

        if xsize % xblk > 0:
            blockaligned = False
            xsize += (xblk - xsize%xblk)
       
        # retrieve data from DVID and pad with data if not block aligned
        # TODO: optimize to minimize data fetches from DVID
        if not blockaligned:
            newdata = self._getchunk(znew, ynew, xnew, zsize, ysize, xsize)
            newdata[(z-znew):(z-znew+zsizeorig),(y-ynew):(y-ynew+ysizeorig),(x-new):(x-xnew+xsizeorig)]
            data = newdata

        # interface is the same for labels and raw arrays but the function is stateless
        # and can benefit from extra compression possible in labels in some use cases
        if self.dtype = ArrayDtype.uint8:
            data = self.ns.put_array8bit(self.instancename, data, (z, y, x), self.islabels3D)
        elif self.dtype = ArrayDtype.uint16:
            data = self.ns.put_array16bit(self.instancename, data, (z, y, x), self.islabels3D)
        elif self.dtype = ArrayDtype.uint32:
            data = self.ns.put_array32bit(self.instancename, data, (z, y, x), self.islabels3D)
        elif self.dtype = ArrayDtype.uint64:
            data = self.ns.put_array64bit(self.instancename, data, (z, y, x), self.islabels3D)
        else:
            raise DicedException("Invalid datatype for array")


    def __setitem__(self, index, val):
        """Use index to set array data.

        Note:
            Large requests are split into several small requests.  Data
            can be sent outside of the extents.
        """
        
        if len(index) > 3:
            raise DicedException("Does not support arrays with dimension greater than 3")

        if self.locked:
            raise DicedException("Cannot write to locked node")

        z = y = x = slice(0,1)
        if len(index) == 3:
            z,y,x = index
        elif len(index) == 2:
            y, x = index
        else:
            x = index

        # only support calls to volumes <= 512x512x512, larger calls should
        # be split into several pieces
        zsize = z.stop - z.start
        ysize = y.stop - y.start
        xsize = x.stop - x.start
        if zsize*ysize*xsize > self.MAX_REQ_SIZE: 
            data = numpy.zeros((zsize, ysize, xsize), dtype.value)
       
            # split into chunks
            zincr = zsize    
            yincr = ysize    
            xincr = xsize    
       
            while zincr*yincr*xincr > self.MAX_REQ_SIZE:
                if zincr > yincr and zincr > xincr:
                    zincr = zincr/2 + zincr % 2
                elif yincr > zincr and yincr > xincr:
                    yincr = yincr/2 + yincr % 2
                else:
                    xincr = xincr/2 + xincr % 2

            for ziter in range(0, zsize, zincr):
                for yiter in range(0, ysize, yincr):
                    for xiter in range(0, xsize, xincr):
                        zstart = ziter + z.start
                        ystart = yiter + y.start
                        xstart = xiter + x.start
                       
                        csizez = min(zsize - zstart, zincr)
                        csizey = min(ysize - ystart, yincr)
                        csizex = min(xsize - xstart, xincr)

                        self._setchunk(zstart, ystart, xstart,
                                val[ziter:ziter+csizez, yiter:yiter+csizey, xiter:xiter+csizex].copy())
        else:
            self._setchunk(zstart, ystart, xstart, val)




        return

