Key-Value (KV) Stores: kv_engine.py

Simple key-value databse, which uses an associative array, ie a map or dictionary, where each key is associated with one value in a collection.

Within each KV pair the key is represented by an arbitrary string, ie a filename, and the value is any from of data (image, document, user preference). The value is a 'blob'
with no upfront data modeling or schema definition

In general the KV srtoes have query language. They provide a way to store, retrive, and update using standard get, put, and delete commands. The path to 
retrieve the data is a direct request to the object in memory.

The store of the KV store is a "mapping of mappings." Essentially self.store is a standard python dict whose keys are collections, and each value is another python dict which is referred to as a bukcet.
The buckets map the keys to the correspondings bytes or 'blobs.'

put()
    Upsert nature --> if the key DOES NOT exist, insert the (key, value), if the key DOES exist overwrite the old blob with the new value
    Atomicity --> Entire write must succeed completely
    Durability/persistence --> WAL, must append [PUT, key, value] to the log BEFORE mutating the in-memory map
    Errors & Exceptions --> InvalidKeyError if the key is empty or to long, ValueToolargeError if the value excees configured max size, IOError if the disk write fails

get()
    Lookup --> hash the key, locate its associated bucket in-memory and return its blob

delete()
    Idempotent Removal --> if the key exists, remove it and record deletion, if key does not exists raise error
