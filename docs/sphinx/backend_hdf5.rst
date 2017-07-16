
.. _hdf5:

HDF5 Backend
==============



Filesystem Layout
-----------------------

- Volume/
	- index.db
	- blocks/
		- (block name 1)/
			- _info.hdf5
				- meta:/dictionary (empty dataset)
					- 
			- _groups/
				- (group name).hdf5
					- meta:/schema
					- meta:/data_TYPE_INT8
					- meta:/data_TYPE_UINT8
					- meta:/data_TYPE_INT16
					- meta:/data_TYPE_UINT16
					- meta:/data_TYPE_INT32
					- meta:/data_TYPE_UINT32
					- meta:/data_TYPE_INT64
					- meta:/data_TYPE_UINT64
					- meta:/data_TYPE_FLOAT32
					- meta:/data_TYPE_FLOAT64 (includes TIDAS_TIME field)
					- meta:/data_TYPE_STRING (fixed width)
			- _intervals/
				- (interval name).hdf5
					- meta:/times
					- meta:/indices
			- _extensions/
				- (extension name).hdf5 (arbitrary contents)
			- (sub block name)/
				- (... arbitrary depth)
		- (block name 2)/
			- (... arbitrary depth)

