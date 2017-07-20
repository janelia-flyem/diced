"""Functionality to store and retrieve data into Diced arrays.
"""

from __future__ import absolute_import
import numpy as np
from enum import Enum
from .DicedException import DicedException

class ArrayDtype(Enum):
    """Defines datatypes supported.
    """
    uint8 = np.uint8
    uint16 = np.uint16
    uint32 = np.uint32
    uint64 = np.uint64


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

    def __init__(self, name, dicedstore, locked, nodeconn, numdims, dtype, islabel3D):
        """Internal Init.

        Init is called by DicedRepo.  It has a pointer to dicedstore
        to ensure DiceStore is active while this instance is around.

        Args:
            name (str): datatype instance
            dicedstore (DicedStore): referenced to DicedStore
            locked (str): if node locked, read only
            nodeconn (libdvid object): connection to DVID version node
            numdims (int): number of dimensions for array
            dtype (ArrayDtype): array datatype
            islabel3D (bool): is a label array type
        """

        self.instancename = name
        self.dicedstore = dicedstore
        self.locked = locked
        self.ns = nodeconn
        self.numdims = numdims
        self.dtype = dtype
        self.islabel3D = islabel3D

        # extract specific meta
        allmeta = self.ns.get_typeinfo(self.instancename)
        self.blocksize = allmeta["Extended"]["BlockSize"]
   
    def get_numdims(self):
        """Retrieves the number of dimensions.

        Returns:
            int indicating the number of dimensions
        """

        return self.numdims

    def get_extents(self):
        """Retrieve extants for array.

        This shows the extent of data written aligned to
        the internal chunk storage size.

        Returns:
            tuple of slices
        """

        # return extents (z,y,x)
        val = self.ns.get_typeinfo(self.instancename)
        
        minpoint = val["Extended"]["MinPoint"]
        xs = ys = zs = 0
        if minpoint is not None:
            xs,ys,zs = val["Extended"]["MinPoint"]
        
        maxpoint = val["Extended"]["MaxPoint"]
        xf = yf = zf = -1
        if maxpoint is not None:
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
        if self.dtype == ArrayDtype.uint8:
            data = self.ns.get_array8bit3D(self.instancename, (zsize, ysize, xsize), (z, y, x), self.islabel3D)
        elif self.dtype == ArrayDtype.uint16:
            data = self.ns.get_array16bit3D(self.instancename, (zsize, ysize, xsize), (z, y, x), self.islabel3D)
        elif self.dtype == ArrayDtype.uint32:
            data = self.ns.get_array32bit3D(self.instancename, (zsize, ysize, xsize), (z, y, x), self.islabel3D)
        elif self.dtype == ArrayDtype.uint64:
            data = self.ns.get_array64bit3D(self.instancename, (zsize, ysize, xsize), (z, y, x), self.islabel3D)
        else:
            raise DicedException("Invalid datatype for array")

        return data


    def __getitem__(self, index):
        """Use index to retrieve array data.
        
        Note:
            Large requests are split into several small requests.  Data
            can be requested outside of the extents.
        """

        dimsreq = 1
        # handle query of single point
        singleindex1 = False
        singleindex2 = False
        singleindex3 = False

        if type(index) == int:
            index = slice(index, index+1)
            singleindex3 = True
        elif type(index) != slice:
            dimsreq = len(index)
            if dimsreq == 3:
                a, b, c = index
                if type(a) == int:
                    a = slice(a, a+1)
                    singleindex1 = True
                if type(b) == int:
                    b = slice(b, b+1)
                    singleindex2 = True
                if type(c) == int:
                    c = slice(c, c+1)
                    singleindex3 = True
                index = (a, b, c)
            if dimsreq == 2:
                a, b = index
                if type(a) == int:
                    a = slice(a, a+1)
                    singleindex2 = True
                if type(b) == int:
                    b = slice(b, b+1)
                    singleindex3 = True
                index = (a, b)

        if self.numdims != dimsreq:
            raise DicedException("Array has a different number of dimensions than requested")

        z = y = x = slice(0,1)
        if dimsreq == 3:
            z,y,x = index
        elif dimsreq == 2:
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
            data = np.zeros((zsize, ysize, xsize), self.dtype.value)
       
            # split into chunks
            zincr = zsize    
            yincr = ysize    
            xincr = xsize    
     
            while zincr*yincr*xincr > self.MAX_REQ_SIZE:
                if zincr > yincr and zincr > xincr:
                    zincr = zincr//2 + zincr % 2
                elif yincr > zincr and yincr > xincr:
                    yincr = yincr//2 + yincr % 2
                else:
                    xincr = xincr//2 + xincr % 2

            for ziter in range(0, zsize, zincr):
                for yiter in range(0, ysize, yincr):
                    for xiter in range(0, xsize, xincr):
                        zstart = ziter + z.start
                        ystart = yiter + y.start
                        xstart = xiter + x.start
                       
                        csizez = min(z.stop - zstart, zincr)
                        csizey = min(y.stop - ystart, yincr)
                        csizex = min(x.stop - xstart, xincr)

                        tdata = self._getchunk(zstart, ystart, xstart, csizez, csizey, csizex)
                        data[ziter:ziter+csizez, yiter:yiter+csizey, xiter:xiter+csizex] = tdata
        else:
            # small call can be had in one call
            data = self._getchunk(z.start, y.start, x.start, zsize, ysize, xsize)

        # squeeze data as requested
        if self.numdims == 3:
            if singleindex1 and singleindex2 and singleindex3:
                data = int(data.squeeze())
            elif singleindex1 and singleindex2:
                data = data.squeeze((0,1))
            elif singleindex2 and singleindex3:
                data = data.squeeze((1,2))
            elif singleindex1 and singleindex3:
                data = data.squeeze((0,2))
            elif singleindex1:
                data = data.squeeze(0)
            elif singleindex2:
                data = data.squeeze(1)
            elif singleindex3:
                data = data.squeeze(2)

        elif self.numdims == 2:
            data = data.squeeze(0)

            if singleindex2 and singleindex3:
                data = int(data.squeeze())
            elif singleindex2:
                data = data.squeeze(0)
            elif singleindex3:
                data = data.squeeze(1)

        elif self.numdims == 1:
            data = data.squeeze((0,1))
            if singleindex3:
                data = int(data.squeeze)

        return data
    
    def _setchunk(self, z, y, x, data):
        """Internal function to set data.
        """
       
        # does not support setting individual elements
        # (inefficiently accesses diced store)
        if type(data) != np.ndarray:
            raise DicedException("Must set data using a numpy array")
        
        # check if block aligned and adjust extents
        xblk, yblk, zblk = self.blocksize
        zsize = ysize = xsize = 1
        if len(data.shape) == 3:
            zsize, ysize, xsize = data.shape
        elif len(data.shape) == 2:
            ysize, xsize = data.shape
        elif len(data.shape) == 1:
            xsize = data.shape[0]
       
        zsizeorig = zsize
        ysizeorig = ysize
        xsizeorig = xsize
        
        blockaligned = True
    
        znew = z - (z%zblk)
        ynew = y - (y%yblk)
        xnew = x - (x%xblk)
       
        zsize += (z%zblk)
        ysize += (y%yblk)
        xsize += (x%xblk)

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
            newdata[(z-znew):(z-znew+zsizeorig),(y-ynew):(y-ynew+ysizeorig),(x-xnew):(x-xnew+xsizeorig)] = data
            data = newdata

        # interface is the same for labels and raw arrays but the function is stateless
        # and can benefit from extra compression possible in labels in some use cases
        if self.dtype == ArrayDtype.uint8:
            data = self.ns.put_array8bit3D(self.instancename, data, (znew, ynew, xnew), self.islabel3D)
        elif self.dtype == ArrayDtype.uint16:
            data = self.ns.put_array16bit3D(self.instancename, data, (znew, ynew, xnew), self.islabel3D)
        elif self.dtype == ArrayDtype.uint32:
            data = self.ns.put_array32bit3D(self.instancename, data, (znew, ynew, xnew), self.islabel3D)
        elif self.dtype == ArrayDtype.uint64:
            data = self.ns.put_array64bit3D(self.instancename, data, (znew, ynew, xnew), self.islabel3D)
        else:
            raise DicedException("Invalid datatype for array")


    def __setitem__(self, index, val):
        """Use index to set array data.

        Note:
            Large requests are split into several small requests.  Data
            can be sent outside of the extents.
        """
       
        dimsreq = 1
        if type(index) == int:
            # handle query of single point
            index = slice(index, index+1)
        elif type(index) != slice:
            dimsreq = len(index)
            if dimsreq == 3:
                a, b, c = index
                if type(a) == int:
                    a = slice(a, a+1)
                if type(b) == int:
                    b = slice(b, b+1)
                if type(c) == int:
                    c = slice(c, c+1)
                index = (a, b, c)
            if dimsreq == 2:
                a, b = index
                if type(a) == int:
                    a = slice(a, a+1)
                if type(b) == int:
                    b = slice(b, b+1)
                index = (a, b)


        if self.numdims != dimsreq:
            raise DicedException("Array has a different number of dimensions than requested")

        if self.locked:
            raise DicedException("Cannot write to locked node")

        z = y = x = slice(0,1)
        if dimsreq == 3:
            z,y,x = index
        elif dimsreq == 2:
            y, x = index
        else:
            x = index

        # only support calls to volumes <= 512x512x512, larger calls should
        # be split into several pieces
        zsize = z.stop - z.start
        ysize = y.stop - y.start
        xsize = x.stop - x.start
        if zsize*ysize*xsize > self.MAX_REQ_SIZE: 
            data = np.zeros((zsize, ysize, xsize), self.dtype.value)
            # split into chunks
            zincr = zsize    
            yincr = ysize    
            xincr = xsize    
       
            while zincr*yincr*xincr > self.MAX_REQ_SIZE:
                if zincr > yincr and zincr > xincr:
                    zincr = zincr//2 + zincr % 2
                elif yincr > zincr and yincr > xincr:
                    yincr = yincr//2 + yincr % 2
                else:
                    xincr = xincr//2 + xincr % 2

            for ziter in range(0, zsize, zincr):
                for yiter in range(0, ysize, yincr):
                    for xiter in range(0, xsize, xincr):
                        zstart = ziter + z.start
                        ystart = yiter + y.start
                        xstart = xiter + x.start
                       
                        csizez = min(z.stop - zstart, zincr)
                        csizey = min(y.stop - ystart, yincr)
                        csizex = min(x.stop - xstart, xincr)

                        self._setchunk(zstart, ystart, xstart,
                                val[ziter:ziter+csizez, yiter:yiter+csizey, xiter:xiter+csizex].copy())
        else:
            self._setchunk(z.start, y.start, x.start, val)

        return

