import malevich_coretools as c
import os

c.update_core_credentials(username=os.environ.get("CORE_USER"), password=os.environ.get("CORE_PASS"))
c.set_host_port(os.environ.get("CORE_HOST"))
for i in c.get_run_active_runs().ids:
    try: 
        c.task_stop(i)
    except:
        pass

c.set_host_port(os.environ.get("COREV2_HOST"))
for i in c.get_run_active_runs().ids:
    try: 
        c.task_stop(i)
    except:
        pass