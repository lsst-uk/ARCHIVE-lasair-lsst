# sudo apt-get update
# sudo DEBIAN_PRIORITY=low apt-get install postfix

# choose internet site
# name is filter1
# postmaster roy@roe.ac.uk
# Other destinations -- blank
# synchronous updates -- no
# Local networks -- the default
# mailbox size -- 0
# Local address extension character -- nothing
# Internet protocols -- all





from run_active_queries import send_email
email = 'roydavidwilliams@gmail.com'
topic = 'test'
message = 'hello world'
send_email(email, topic, message)
