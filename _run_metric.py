import subprocess, sys
r = subprocess.run([sys.executable, "metrics/metric_trafo_loading.py"], capture_output=True, text=True, cwd=r"C:\Users\anton\Desktop\project\Nando_pp")
print("STDOUT:", r.stdout[:3000] if r.stdout else "(empty)")
print("STDERR:", r.stderr[:3000] if r.stderr else "(empty)")
print("Return code:", r.returncode)
