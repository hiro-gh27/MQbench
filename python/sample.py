import subprocess
import paramiko


def sample():
    print(subprocess.call("pwd".split()))
    cmd = "ls"
    runcmd = subprocess.call(cmd.split())
    print(runcmd)
    return


def ssh_sample():
    client = paramiko.SSHClient();
    client.set_missing_host_key_policy(paramiko.WarningPolicy())
    client.connect('133.101.34.213', username='hiroki', password='a;sdsj')

    stdin, stdout, stderr = client.exec_command('ls')
    for line in stdout:
        print(line)
    client.close()
    return


if __name__ == '__main__':
    ssh_sample()
