# DVIDCloudStore [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/HHMI_Janelia_Color_Alternate_180x40.png)](http://www.janelia.org)

** (status: not ready) **

This allows one to store large, versioned 3D data in the cloud.

## Tutorial

### Initialization and Installation
* Create Google cloud account.  Create a google project.  Enable billing.
* Install gcloud (TBD: use conda "conda install dvidcloudstore")
* Authorize Google on your machine ('gcloud auth login first')

### Access DVID Volumes
* run 'setup_dvidcloud <project name> <bucket name> <num dvid instances>' (limit 8 instances)
* navigate to the http://IPADDR:8080/#/?admin=1; create a repo if desired
* in python
    % from DVIDVolume import DVIDVolume
    % vol = DVIDVolume("IPADDR:8080", "UUID", "dataname", DVIDVolume.uint8|DVIDVolume.uint16|DVIDVolume.uint32|DVIDVolume.uint64)
    % vol[z0:z1, y0:y1, x0:x1] = numpydata
    % numpydata = vol[z0:z1, y0:y1, x0:x1]
* to commit a node or branch, use the web interface and append 'admin=1' to the URL to have these options to version the data (once data is locked that version can no longer be modified)
* to stop the dvidcloud service call 'destroy_dvidcloud'

## Current limitations

* Permissions are currently not implemented
* Modification of metadata (such as creating new instances and repos or modifying volume extents)
should be done with only 1 DVID node.
* Only supports 3D volumes.  Native support is also only for 8-bit and 64-bit data.
* Writes by default are required to be block aligned.  If the user enables non-block aligned modification,
it is possible that parallel writes could cause inconsistency in the datastore.

## Future Work

* Implement native support for 16 and 32 bit datatypes.
* Implement a separate metadata store to allow multiple writers
* Improve speed of data fetching by maintaining a seperate key store
