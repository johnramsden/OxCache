# Emulate an NVME Device

* Copy `nvme-loop.json.example` to `nvme-loop.json`
* Use an inappropriate backing store, either another block device or a file that has been loop back mounted

Create the nvme device:

```shell
sudo modprobe nvmet nvmet-loop nvme-loop
sudo nvmetcli restore nvme-loop.json 
sudo nvme connect -t loop -n testnqn -q hostnqn 
lsblk
``` 

You should now have a new nvme device appropriate for testing.