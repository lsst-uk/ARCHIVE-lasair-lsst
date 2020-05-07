ansible-playbook --inventory-file=hosts.yml everyone.yml
ansible-playbook --inventory-file=hosts.yml kafka.yml

ansible-playbook --inventory-file=hosts.yml git_pull.yml
ansible-playbook --inventory-file=hosts.yml start_ingest.yml

