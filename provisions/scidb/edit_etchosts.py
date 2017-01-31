with open("/etc/hosts", "rb") as f:
	lines = f.readlines()
	lines[1] = "#::1    localhost ip6-localhost ip6-loopback"
with open("/etc/hosts", "wb") as f:
	f.writelines(lines)