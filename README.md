# PubSubEmulator

This is an emulator to be used with the GCP pubsub client.  It is very similar to the GCP pubsub-emulator except it is not written in java so it doesnt suck for memory usage as much.  It also supposed a basic interface for different backends so it can do some limited persistence of messages/topics/subs.

Its important to note this is NOT a production level service.  This is meant to be a more stable replacement for the GCP pubsub-emulator.  Its mostly only used for testing and development environments.  Do not submit any issues around security, scalibility or HA.  

## Memory backend

This is the default backend, all topic/subs/messages are kept in memory and will be lost on restart.  This is extremly fast and good for simple intigration tests.

### Running memory backend

You can start the memory backend with docker w/o providing any additional arguments:

```bash

docker run -it --rm -p 8681:8681 lwahlmeier/pse:0.0.1

```

Once started you can connect your GCP client by setting the following environment variable before starting it:
```bash

export PUBSUB_EMULATOR_HOST=127.0.0.1:8681

```


## FileSystem backend

This is meant to have alittle more persistence around topic/subs/message state.  It can survive a restart or crash and keep most state.  This is made more for testing e2e type environments where you have more moving parts/services and a bit more resiliancy then what the memory based backend provides.  This is pretty simple and should be relativly fast assuming the backing disk is not to slow. 

### Running the FileSystem backend

You must set the backend type and set the location you want to store the data from it.  

```bash

docker run -it --rm -p 8681:8681 -e PSE_TYPE=filesystem -e PSE_FSPATH=/pubsub -v /tmp/pubsub2:/pubsub lwahlmeier/pse:0.0.1

```

This will write all pubsub data to `/tmp/pubsub` on the host.  If you add topics/subs/messages then restart pse you should maintaine state for all supported data.
