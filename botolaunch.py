import time
import boto
from boto.ec2.regioninfo import RegionInfo

# Providers to be given to the harvester instances
providers = ['VodafoneAU', 'Telstra', 'Optus']

# Connect to the ec2 endpoint using our AWS credentials
region = RegionInfo(name = 'melbourne', endpoint = 'nova.rc.nectar.org.au')
ec2_conn = boto.connect_ec2(aws_access_key_id = '09224b91871f49848d8dbf472d984f03',
			aws_secret_access_key = '5998d7d42fbd40b7b9446538d42096fb',
			is_secure = True,
			region = region,
			port = 8773,
			path = '/services/Cloud',
			validate_certs = False)
print 'Connected to NeCTAR'

# Launch the main instance
ec2_conn.run_instances('ami-00003840',
			key_name = 'Cloud',
			instance_type = 'm1.large',
			security_groups = ['default', 'SSH', 'CouchDB'],
			placement = 'melbourne-np')
print 'Launching Main Instance'
main = ec2_conn.get_all_reservations()[0].instances[0]

# Create a storage volume
vol_req = ec2_conn.create_volume(200, 'melbourne-np')
curr_vol = ec2_conn.get_all_volumes([vol_req.id])[0]

# Wait until the main instance is running, then attach the volume
while main.state != 'running':
	time.sleep(1)
	main.update()
ec2_conn.attach_volume(curr_vol, main.id, '/dev/vdc')
print 'Attached Volume to Main Instance'

# Create 3 small harvester instances
for i in range(0, 3):
	ec2_conn.run_instances('ami-00003840',
			key_name = 'Cloud',
			instance_type = 'm1.small',
			security_groups = ['default', 'SSH', 'CouchDB'],
			placement = 'melbourne-np')
	print 'Launching Harvester Instance', i+1

# Open the Ansible hosts file to write instance IP addresses
f = open('/etc/ansible/hosts', 'w')

f.write('[main]\n')
f.write(main.ip_address + '\n')
f.write('\n')

f.write('[harvesters]\n')
n = 0
for i in range(0, 4):
	instance = ec2_conn.get_all_reservations()[i].instances[0]
	while instance.state != 'running':
		time.sleep(1)
		instance.update()

	if instance.ip_address != main.ip_address:
		f.write(instance.ip_address + ' provider=' + providers[n] + '\n')
		n = n + 1

f.write('\n')
f.write('[harvesters:vars]\n')
f.write('main_ip=' + main.ip_address + '\n')

f.close()
