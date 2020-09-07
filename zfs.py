import subprocess
import logging
import re
import asyncio
from trainml.services import ipfs


def list_datasets(starts_with=None):
    command = ["zfs", "list", "-o", "name"]
    if starts_with:
        command.append("-r")
        command.append(starts_with)
    result = subprocess.run(command, capture_output=True, encoding="utf-8",)
    if result.returncode != 0:
        raise Exception("Error: " + result.stderr)
    output_list = result.stdout.split("\n")
    output_list.pop(0)
    output_list.remove("")
    return output_list


def get_dataset_status(dataset):
    options_list = "name,used,avail,refer,encryptionroot,quota,mounted,mountpoint"
    result = subprocess.run(
        ["zfs", "list", "-o", options_list, dataset],
        capture_output=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise Exception("Error: " + result.stderr)
    lines = result.stdout.split("\n")
    header = [col.lower() for col in lines[0].split(" ") if col != ""]
    data = [col for col in lines[1].split(" ") if col != ""]
    output = {header[i]: data[i] for i in range(len(header))}
    return output


def create_dataset(name, mountpoint, key, quota):
    result = subprocess.run(
        [
            "sudo",
            "zfs",
            "create",
            "-o",
            "canmount=noauto",
            "-o",
            "encryption=aes-256-gcm",
            "-o",
            "keylocation=prompt",
            "-o",
            "keyformat=passphrase",
            "-o",
            f"quota={quota}",
            "-o",
            f"mountpoint={mountpoint}",
            name,
        ],
        capture_output=True,
        input=key,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise Exception("Invalid dataset creation: " + result.stderr)
    output = get_dataset_status(name)
    return output


def load_dataset_key(dataset, key):
    result = subprocess.run(
        ["sudo", "zfs", "load-key", dataset],
        capture_output=True,
        input=key,
        encoding="utf-8",
    )
    if result.returncode != 0:
        pattern = "Key load error: Key already loaded for(.*)"
        check_for_pattern = re.search(pattern, str(result.stderr))
        if check_for_pattern == None:
            raise Exception("Error: " + result.stderr)


def mount_dataset(dataset):
    result = subprocess.run(
        ["sudo", "zfs", "mount", dataset], capture_output=True, encoding="utf-8"
    )
    if result.returncode != 0:
        raise Exception("Error: " + result.stderr)


def unmount_dataset(dataset, force=False):
    command = ["sudo", "zfs", "unmount"]
    if force:
        command.append("-f")
    command.append(dataset)
    result = subprocess.run(command, capture_output=True, encoding="utf-8")
    if result.returncode != 0:
        if not force:
            raise Exception("Error: " + result.stderr)


def destroy_dataset(dataset, force=False):
    command = ["sudo", "zfs", "destroy", "-R"]
    if force:
        command.append("-f")
    command.append(dataset)
    result = subprocess.run(command, capture_output=True, encoding="utf-8")
    if result.returncode != 0:
        if not force:
            raise Exception("Error: " + result.stderr)


async def create_snapshot(dataset, name):
    logging.info(f"Begin create snapshot {name} of {dataset}")
    command = f"sudo zfs snapshot {dataset}@{name}"
    proc = await asyncio.create_subprocess_shell(
        command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    logging.info(f"Finish create snapshot {name} of {dataset}")

    if proc.returncode != 0:
        raise Exception("Error: " + stderr.decode())


async def send_snapshot(snapshot, file_path):
    logging.info(f"Begin send snapshot {snapshot} to {file_path}")
    f = open(file_path, "w")
    command = f"sudo zfs send {snapshot}"
    proc = await asyncio.create_subprocess_shell(
        command, stdout=f, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()

    logging.info(f"Finish send snapshot {snapshot} to {file_path}")
    if proc.returncode != 0:
        raise Exception("Error: " + stderr.decode())


async def create_clone(snapshot, dataset, quota):
    command = f"sudo zfs clone -o quota={quota} -o canmount=noauto {snapshot} {dataset}"
    proc = await asyncio.create_subprocess_shell(
        command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        raise Exception("Error: " + stderr.decode())


async def send_snapshot_to_ipfs(snapshot, callback):
    logging.info(f"Begin send snapshot {snapshot} to ipfs")
    container_id = ipfs.get_container_id()
    # size-1048576
    # rabin-524288-786432-1048576
    #
    command = f"sudo zfs send -Rw {snapshot} | docker exec -i {container_id} ipfs add -s buzhash"
    proc = await asyncio.create_subprocess_shell(
        command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        raise Exception("Error: " + stderr.decode())
    else:
        logging.info(f"Finish send snapshot {snapshot} to ipfs: {stdout.decode()}")
        cid = stdout.decode().split(" ")[1]
        logging.info(f"cid: {cid}")
        callback(cid)


async def receive_dataset_from_ipfs(cid, dataset, callback):
    logging.info(f"Begin receive dataset {dataset} from ipfs")
    container_id = ipfs.get_container_id()
    command = (
        f"docker exec -i {container_id} ipfs cat {cid} | sudo zfs receive {dataset} "
    )
    proc = await asyncio.create_subprocess_shell(
        command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        raise Exception("Error: " + stderr.decode())
    else:
        logging.info(f"Finish receive dataset {dataset} from ipfs: {stdout.decode()}")
        status = get_dataset_status(dataset)
        logging.info(f"dataset status: {status}")
        callback()
