"""Functonality to access a specific version of a repository.
"""

from __future__ import absolute_import
import sys
import json
import subprocess

from libdvid import DVIDNodeService, ConnectionMethod, DVIDConnection
from libdvid._dvid_python import DVIDException
from .DicedException import DicedException
from .DicedArray import DicedArray
from .DicedArray import ArrayDtype

if sys.version_info.major >= 3:
    unicode = str

class DicedRepo(object):
    """Provides access to a version of the specified repo.

    Note:
        If the version of the repo has been locked (i.e., is not open),
        creating datainstance and writing data is disabled.  If one
        wants to write to the repo, a new node should be branched.
    
        There are several several metadata conventions that are
        used by diced.

            * files are always located in '.files'
            * .meta contains various information about repo
            * .meta/restrictions: is a list of instance names to be ignored by default (set outside of diced)
            * .meta/instance:{instancename:datauuid}: contains '{"numdims": <num>}'
            * .meta/neuroglancer: contains a list of instances compatible with neuroglancer

    """ 
    
    # block size constants
    BLKSIZE3D = 64
    BLKSIZE2D = 512
    BLKSIZE1D = 262144
   
    # keep track of all internal DVID datatypes supported
    SupportedTypes = {"uint8blk" : ArrayDtype.uint8, "uint16blk": ArrayDtype.uint16,
            "uint32blk": ArrayDtype.uint32, "uint64blk": ArrayDtype.uint64,
            "labelblk": ArrayDtype.uint64}
    RawTypeMappings = {ArrayDtype.uint8: "uint8blk", ArrayDtype.uint16: "uint16blk",
                        ArrayDtype.uint32: "uint32blk", ArrayDtype.uint64: "uint64blk"} 
    LabelTypeMappings = {ArrayDtype.uint8: "labelblk", ArrayDtype.uint16: "labelblk",
                        ArrayDtype.uint32: "labelblk", ArrayDtype.uint64: "labelblk"} 

    LabelTypes = set(["labelblk"])
   
    MetaLocation = ".meta"
    FilesLocation = ".files"
    RestrictionName = "restrictions"
    InstanceMetaPrefix = "instance:"

    def __init__(self, server, uuid, dicedstore):
        self.server = server
        
        # create connection object to DVID
        self.rawconn = DVIDConnection(server) 
        
        # hold reference
        self.dicedstore = dicedstore

        self.uuid = None
        
        # DVID version node connection
        self.nodeconn = None

        # all instances to ignore for ls
        self.hidden_instances = None

        # fetch all repo information
        self.repoinfo = None
        # whether current version is read only
        self.locked = None
       
        # meta for current node
        self.current_node = None

        # instances available at version
        self.allinstances = None
        self.activeinstances = None
        
        # initialize version specific data
        self._init_version(uuid)

        # create meta types if not currently available
        if self.MetaLocation not in self.activeinstances:
            self.nodeconn.create_keyvalue(self.MetaLocation)
        if self.FilesLocation not in self.activeinstances:
            self.nodeconn.create_keyvalue(self.FilesLocation)

    def change_version(self, uuid):
        """Change the current node version.

        Args:
            uuid (str): version node to use
        
        Raises:
            DicedException if UUID not found
        """
        
        for tuuid in self.alluuids:
            if tuuid.startswith(uuid):
                self._init_version(uuid) 
                return
        raise DicedException("UUID not found")

    def get_current_version(self):
        """Retrieve the current version ID.

        Returns:
            String for UUID
        """

        return self.uuid

    def get_array(self, name):
        """Retrive a DicedArray object.

        Returns:
            DicedArray

        Raises:
            DicedException if the array does not exist
        """

        if name in self.activeinstances:
            # check if accepted type
            typename = self.activeinstances[name]
            if typename in self.SupportedTypes: # only support arrays
                islabel3D = typename in self.LabelTypes
                numdims = 3

                # check if num dims specified
                try:
                    datauuid = self.repoinfo["DataInstances"][name]["Base"]["DataUUID"]
                    data = self.nodeconn.get_json(self.MetaLocation, str(self.InstanceMetaPrefix+name+":"+datauuid))
                    numdims = data["numdims"]
                except:
                    pass

                return DicedArray(name, self.dicedstore, self.locked, self.nodeconn, 
                        numdims, self.SupportedTypes[typename], islabel3D)
            else:
                raise DicedException("Instance name: " + name + " has an unsupported type " + typename)
        
        raise DicedException("Instance name: " + name + " not found in version " + self.uuid)


    def create_array(self, name, dtype, dims=3, islabel3D=False,
            lossycompression=False, versioned=True): 
        """Create a new array in the repo at this version.

        Args:
            name (str): unique name for this array
            dtype (ArrayDtype): datatype (not relevant if labels)
            dims (int): number of dimensions (support 1,2,3)
            islabel3D (bool): treat as label data (usually highly compressible) (always 64bit)
            versioned (bool): allow array to be versioned
            lossycompresion (bool): use lossy compression (only if not islabel3D)

        Returns:
            new DicedArray object

        Raises:
            DicedException if node is locked or name is already used.
        """

        if self.locked:
            raise DicedException("Cannot create instance on locked node")
       
        for (tname, uuid) in self.allinstances:
            if tname == name:
                raise DicedException("Name already exists in repo")

        if islabel3D and dims != 3:
            raise DicedException("islabel3D only supported for 3D data")
        
        if islabel3D and dtype != ArrayDtype.uint64:
            raise DicedException("islabel3D only works with 64 bit data")

        endpoint = "/repo/" + self.uuid + "/instance"
        typename = self.RawTypeMappings[dtype]
        if islabel3D:
            typename = self.LabelTypeMappings[dtype]

        # handle blocksize
        blockstr = str(self.BLKSIZE3D) + "," + str(self.BLKSIZE3D) + "," + str(self.BLKSIZE3D)
        if dims == 2:
            blockstr = str(self.BLKSIZE2D) + "," + str(self.BLKSIZE2D) + "," + str(1)
        if dims == 1:
            blockstr = str(self.BLKSIZE1D) + "," + str(1) + "," + str(1)

        data = {"typename": typename, "dataname": name, "BlockSize": blockstr}
        
        if not islabel3D and lossycompression:
            data["Compression"] = "jpeg"

        self.rawconn.make_request(endpoint, ConnectionMethod.POST, json.dumps(data).encode('utf-8'))

        # update current node meta 
        self._init_version(self.uuid)
        
        # use '.meta' keyvalue to store array size (since not internal to DVID yet)
        self.nodeconn.put(self.MetaLocation, self.InstanceMetaPrefix+name+":"+str(self.repoinfo["DataInstances"][name]["Base"]["DataUUID"]), json.dumps({"numdims": dims}).encode('utf-8'))

        return DicedArray(name, self.dicedstore, False, self.nodeconn, 
            dims, dtype, islabel3D)

    def get_commit_log(self):
        """Retrieve a list of commit log messages.
        
        Returns:
            [(str, str] array of uuid, log messages
        """

        return self.loghistory 
        

    def upload_filedata(self, dataname, data):
        """Upload file data to this repo version.

        Args:
            dataname (str): name of file
            data (str): data to store
        
        Raises:
            DicedException if locked
        """

        if self.locked:
            raise DicedException("Node already locked")
        if isinstance(data, unicode):
            data = data.encode('utf-8')
        self.nodeconn.put(self.FilesLocation, dataname, data)


    def download_filedata(self, dataname):
        """Download file data.

        Args:
            dataname (str): name of file
           
        Returns:
            Data for file as a string

        Raises:
            DicedException if file does not exist
        """

        data = None
        try:
            data = self.nodeconn.get(self.FilesLocation, dataname)
        except DVIDException:
            raise DicedException("file does not exist")
        return data

    def list_instances(self, showhidden=False):
        """Lists data store in this version of the repo.

        This will show all the array data.  The user can
        choose to show everything shown in DVID.

        Args:
            showhidden (boolean): show all instances (even non-array)
       
        Return:
            [ (instance name, ArrayDtype) ] if showhidden is True
            the function returns [ (instance name, type name string) ].

        """
        
        res = []
        for instance, typename in self.activeinstances.items():
            if showhidden:
                res.append((instance, typename))
            elif typename in self.SupportedTypes and instance not in self.hidden_instances:
                res.append((instance, self.SupportedTypes[typename]))
    
        return res

    def list_files(self):
        """List all the files for this version node.
        
        Returns:
            List of strings for file names
        """

        json_text = self.nodeconn.custom_request("/" + self.FilesLocation + "/keys/0/z", b"",
            ConnectionMethod.GET)
        return json.loads(json_text)

    def create_branch(self, message):
        """Create a new branch from this locked node.

        Args:
            message (str): commit message
        
        Returns:
            string for new version UUID            

        Raises:
            DicedExcpetion if node is not locked.
        """
        
        if not self.locked:
            raise DicedException("Must lock node before branching")
        
        res = self.nodeconn.custom_request("/branch",
                                           json.dumps({"note": message}).encode('utf-8'),
                                           ConnectionMethod.POST)
        res = json.loads(res)

        #  add new uuid (no need to reinit everything)
        self.alluuids.add(str(res["child"]))

        return str(res["child"])
        

    def lock_node(self, message):
        """Lock node.

        Args:
            message (str): commit message
        
        Returns:
            string for new version UUID            

        Raises:
            DicedExcpetion if node is already locked.
        """
        
        if self.locked:
            raise DicedException("Node already locked")
        
        self.nodeconn.custom_request("/commit",
                                     json.dumps({"note": message}).encode('utf-8'),
                                     ConnectionMethod.POST)

        # no need to reinit everything
        self.locked = True
   
    def delete_file(self, filename):
        """Delete file from this version.

        This will only delete the file from the current version.
        
        Args:
            filename (str): name of file
        
        Raises:
            DicedExcpetion if node is already locked.
        """
        
        if self.locked:
            raise DicedException("Node already locked")

        self.nodeconn.custom_request("/" + self.FilesLocation + "/key/" + filename, 
                b"", ConnectionMethod.DELETE)
        
    def delete_array(self, dataname):
        """Delete array from repo (not just version!) -- this cannot be undone!

        Note:
            For large arrays this could be very time-consuming.
            While this is non-blocking, currently, DVID
            will not resume the deletion on restart, so it is
            possible for data to still be stored even if it
            is superficially removed.  For now, the user should
            ensure DicedStore is open for a some time after
            issue the command.

        TODO:
            Implement a restartable background delete.

        """

        addr = self.dicedstore._server.split(':')[0]
        rpcaddress = addr + ":" + str(self.dicedstore.rpcport)
        deletecall = subprocess.Popen(['dvid', '-rpc='+rpcaddress, 'repo', self.uuid, 'delete', dataname], stdout=None)
        deletecall.communicate()

        self._init_version(self.uuid)

    def _init_version(self, uuid):
        # create connection to repo
        self.nodeconn = DVIDNodeService(str(self.server), str(uuid))
        
        # fetch all repo information
        status, data, errmsg = self.rawconn.make_request("/repo/" + uuid + "/info", ConnectionMethod.GET)
        self.repoinfo = json.loads(data)
        self.allinstances = {}
        # load all versions in repo
        self.alluuids = set()
        dag = self.repoinfo["DAG"]["Nodes"]
        for uuidt, nodedata in dag.items():
            self.alluuids.add(str(nodedata["UUID"]))
        
        for instancename, val in self.repoinfo["DataInstances"].items():
            # name is not necessarily unique to a repo
            self.allinstances[(str(instancename), str(val["Base"]["DataUUID"]))] = str(val["Base"]["TypeName"])
        

        # datainstances that should be hidden (array of names) 
        try:
            self.hidden_instances = set(self.nodeconn.get_json(self.MetaLocation, self.RestrictionName))
        except:
            self.hidden_instances = set()
        
        nodeids = {}
        # check if locked note
        dag = self.repoinfo["DAG"]["Nodes"]
        for tuuid, nodedata in dag.items():
            nodeids[str(nodedata["VersionID"])] = nodedata
            if tuuid.startswith(uuid):
                self.uuid = str(tuuid)
                self.current_node = nodedata
                self.locked = nodedata["Locked"]
           
        # load all ancestors
        ancestors = set()

        # commit history uuid, commit note in order from oldest to newest
        self.loghistory = []
        currnode = self.current_node
        while True:
            ancestors.add(str(currnode["UUID"]))
            self.loghistory.append((str(currnode["UUID"]), currnode["Note"]))
            if len(currnode["Parents"]) > 0:
                currnode = nodeids[str(currnode["Parents"][0])] 
            else:
                break
        self.loghistory.reverse()

        # load all instances
        self.activeinstances = {}
        
        if not self.locked:
            tempuuid = self.loghistory[-1][0]
            self.loghistory[-1] = (tempuuid, "(open node)")

        for instancename, val in self.repoinfo["DataInstances"].items():
            if str(val["Base"]["RepoUUID"]) in ancestors:
                self.activeinstances[str(instancename)] = str(val["Base"]["TypeName"])

