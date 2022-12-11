import boto3
import datetime
import sys
import os
import re
import time
import logging


"""
Algorithm:
    * disable cluster-autoscaler
    * create new version for worker Launch Template with new AMI
    * get current k8s nodes (old_nodes) and kubectl taint (old_nodes)
    * double scale up ASG and enable scale-in protection for all instances
    * to be sure that new k8s nodes are ready (timeout 5 minutes)
    * check jobs pods on old k8s nodes are exist in specific namespace
     (timeout 2 hours)
    * kubectl drain specific old node
    * disable scale-in protection specific old instance 
    * scale down ASG -1
    * disable scale-in protection for rest instances
    * enable cluster-autoscaler
"""


AWS_ACCOUNT = os.environ.get('AWS_ACCOUNT')
REGION = os.environ.get('REGION')
ENV = os.environ.get('ENV')
AMI_ID = os.environ.get('AMI_ID')
AMI_DESCRIPTION = os.environ.get('AMI_DESCRIPTION')

ROLE = f"arn:aws:iam::{AWS_ACCOUNT}:role/poc_redeploy_nodes-{ENV}"
K8S_ROLE = f"arn:aws:iam::{AWS_ACCOUNT}:role/poc_eks_admin_role-{ENV}"
LT = f"poc_eks_worker_lt-{ENV}"
ASG = f"poc_eks_worker-{ENV}"
K8S_CLUSTER_NAME = f"poc_eks-{ENV}"
K8S_JOBS = ['somepipeline', 'scraper']
K8S_JOBS_FILTERS = {'namespace': ENV, 'jobs': K8S_JOBS}


logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')


def auth():
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html
    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=ROLE,
        DurationSeconds=60 * 60 * 4,  # no less than timeout job + packer build ~1 hour
        RoleSessionName="redeploy")
    credentials = assumed_role_object['Credentials']
    # env vars for binary executions
    os.environ["AWS_ACCESS_KEY_ID"] = credentials['AccessKeyId']
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials['SecretAccessKey']
    os.environ["AWS_DEFAULT_REGION"] = REGION
    os.environ["AWS_SESSION_TOKEN"] = credentials['SessionToken']

    config = {
        'region_name': REGION,
        'aws_access_key_id': credentials['AccessKeyId'],
        'aws_secret_access_key': credentials['SecretAccessKey'],
        'aws_session_token': credentials['SessionToken']}
    ec2_client = boto3.client('ec2', **config)
    asg_client = boto3.client('autoscaling', **config)

    return {'ec2': ec2_client, 'asg': asg_client, 'sts': sts_client}


def create_launch_template_version(client, launch_template, ami, description):
    logging.info("Create new Launch Template version")

    filters = [{'Name': 'launch-template-name', 'Values': [launch_template]}]
    response = client.describe_launch_templates(Filters=filters)
    lt_id = response['LaunchTemplates'][0]['LaunchTemplateId']
    lt_version = response['LaunchTemplates'][0]['LatestVersionNumber']

    client.create_launch_template_version(
        LaunchTemplateId=lt_id,
        SourceVersion=str(lt_version),
        VersionDescription=description,
        LaunchTemplateData={'ImageId': ami})

    logging.debug(f"Launch Template ID: {lt_id}, version: {lt_version}")


def run_cmd(cmd):
    # TODO: fix stderr
    output = os.popen(cmd).read()
    logging.debug(f"{cmd}\n{output}\n")
    return output


def gen_k8s_config(region, cluster_name, k8s_role):
    logging.info("Generate kube-config file")
    params = f"--region {region} --name {cluster_name} --role-arn {k8s_role}"
    cmd = f"aws eks update-kubeconfig {params}"
    run_cmd(cmd)


def set_cluster_autoscaler(is_enabled=True):
    logging.info("{} cluster-autoscaler".format(
        "Enable" if is_enabled else "Disable"))
    name = "deployment/cluster-autoscaler-aws-cluster-autoscaler"
    cmd = f"kubectl scale --replicas={int(is_enabled)} {name} -n kube-system"
    run_cmd(cmd)


def get_k8s_nodes():
    # TODO: merge 2 functions
    cmd = "kubectl get nodes --selector='pool=worker'"
    output = run_cmd(cmd)

    regex = r'^ip[^ ]+'
    nodes = re.findall(regex, output, re.MULTILINE)
    return nodes


def get_k8s_nodes_status():
    cmd = "kubectl get nodes --selector='pool=worker'"
    output = run_cmd(cmd)

    regex = r'^(ip[^ ]+)\s+([^ ]+)'
    nodes = re.findall(regex, output, re.MULTILINE)
    return nodes


def taint_k8s_nodes(nodes):
    logging.info("Taint k8s nodes")
    for node in nodes:
        cmd = f"kubectl taint nodes {node} dedicated=worker:NoSchedule"
        run_cmd(cmd)


def drain_k8s_node(node):
    cmd = f'kubectl drain {node} --ignore-daemonsets'
    run_cmd(cmd)


def get_k8s_pods_status(filters, node):
    namespace = filters['namespace']
    params = f"-n {namespace} -o wide --field-selector spec.nodeName={node}"
    cmd = f"kubectl get pods {params}"
    output = run_cmd(cmd)

    # TODO: issue if jobs < 2
    jobs = '|'.join(filters['jobs'])
    regex = rf'^((?:{jobs})[^ ]+)\s+\d+/\d+\s+([^ ]+)'
    match = re.findall(regex, output, re.MULTILINE)

    # TODO: fix output
    return match


def get_instance_id(node):
    cmd = f'kubectl describe node/{node}'
    output = run_cmd(cmd)

    regex = r'^ProviderID:\s+aws:///[-a-zA-Z0-9]+/([^\n]+)'
    instance = re.findall(regex, output, re.MULTILINE)[0]
    return instance


def get_asg_capacity(client, asg):
    logging.info("Get ASG capacity")

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg])
    capacity = {'desired': response['AutoScalingGroups'][0]['DesiredCapacity'],
                'max': response['AutoScalingGroups'][0]['MaxSize']}

    logging.debug(', '.join([f'{k} - {v}' for k, v in capacity.items()]))
    return capacity


def scale_asg(client, capacity, asg):
    logging.info("Scale ASG")

    client.update_auto_scaling_group(
        AutoScalingGroupName=asg,
        MaxSize=capacity['max'],
        DesiredCapacity=capacity['desired'])

    msg = f"Scale ASG: desired - {capacity['desired']}, max - {capacity['max']}"
    logging.debug(msg)


def get_asg_instances(client, asg):
    logging.info("Get ASG instances")

    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg])
    instances = response['AutoScalingGroups'][0]['Instances']

    # TODO: fix print, instances is a dict
    # msg = f"Get ASG instances: " + ", ".join(instances)
    #logging.debug(msg)
    return instances


def set_asg_protection(client, asg, instances, is_enabled=True):

    logging.info("{} scale-in protection for all (rest) nodes".format(
        "Enable" if is_enabled else "Disable"))

    client.set_instance_protection(
        InstanceIds=instances,
        AutoScalingGroupName=asg,
        ProtectedFromScaleIn=is_enabled)

    # TODO: fix output
    # msg = f"Set scale-in protection: " + ", ".join(instances)


def wait_asg_status(client, capacity_cur, capacity_exp, asg, timeouts):
    logging.info("Wait until ASG instances will be InService status")

    while capacity_cur < capacity_exp:
        capacity_cur = len(get_asg_instances(client, asg))

    cur_time = datetime.datetime.now()
    deadline_time = cur_time + datetime.timedelta(seconds=timeouts['deadline'])

    while cur_time <= deadline_time:
        instances_status = [i['LifecycleState'] for i in get_asg_instances(
            client, asg)]

        statuses_uniq = set(instances_status)
        if len(statuses_uniq) == 1 and 'InService' in statuses_uniq:
            break

        time.sleep(timeouts['iteration'])
        cur_time = datetime.datetime.now()
    else:
        msg = f"{timeouts['deadline']} is expired: some EC2 is not InService"
        logging.error(msg)
        sys.exit(1)


def wait_k8s_nodes_status(k8s_nodes, timeouts):
    logging.info("Wait until k8s nodes be in Ready status")

    cur_time = datetime.datetime.now()
    deadline_time = cur_time + datetime.timedelta(seconds=timeouts['deadline'])

    while cur_time <= deadline_time:
        nodes = get_k8s_nodes_status()
        nodes_status = set([node[1] for node in nodes])
        if len(nodes) == k8s_nodes:
            if len(nodes_status) == 1 and 'Ready' in nodes_status:
                break

        time.sleep(timeouts['iteration'])
        cur_time = datetime.datetime.now()
    else:
        msg = f"{timeouts['deadline']} is expired: k8s nodes are not ready"
        logging.error(msg)
        sys.exit(1)


def replace_nodes(asg_client, k8s_jobs_filters, k8s_nodes, timeouts):
    logging.info("Wait 2 hours or until pods controlled by jobs are completed")

    cur_time = datetime.datetime.now()
    deadline_time = cur_time + datetime.timedelta(seconds=timeouts['deadline'])

    while cur_time <= deadline_time:
        """
        wait 2 hours
        get list old nodes in Ready status
        check pods controlled by jobs on the specific node
        kubectl drain specific node
        disable scale-in protection for specific ASG instance
        scale down ASG desired -1, max -10
        """

        # check only old k8s nodes with only Ready status
        nodes = get_k8s_nodes_status()
        # TODO
        k8s_nodes_ready = [node[0] for node in nodes if node[0] in k8s_nodes and node[1] == 'Ready']

        if not k8s_nodes_ready:
            break

        for node in k8s_nodes_ready:
            pods = get_k8s_pods_status(k8s_jobs_filters, node)
            statuses = [pod[1] for pod in pods if pod]

            # TODO: # Did you mean set(statuses) not in ["Running"] ?
            if not pods or set(statuses) in ["Running"]:
                drain_k8s_node(node)

                instances = get_instance_id(node).split()
                set_asg_protection(asg_client, ASG, instances, is_enabled=False)

                cur_capacity = get_asg_capacity(asg_client, ASG)
                new_capacity = {'desired': cur_capacity['desired'] - 1,
                                'max': cur_capacity['max']}
                scale_asg(asg_client, new_capacity, ASG)

        time.sleep(timeouts['iteration'])
        cur_time = datetime.datetime.now()
    else:
        """
        in case 2 hours timeout
        drain rest nodes
        disable scale-in protection for all instances
        scale down (max -10, desired -1) on rest instances in ASG  
        """

        logging.warning(f"{timeouts['deadline']} timeout for jobs is expired")

        nodes = get_k8s_nodes_status()
        # TODO
        k8s_nodes_ready = [node[0] for node in nodes if node[0] in k8s_nodes and node[1] == 'Ready']
        logging.info("Force drain nodes:", k8s_nodes_ready)
        for node in k8s_nodes_ready:
            drain_k8s_node(node)

        instances = [get_instance_id(node) for node in k8s_nodes_ready]
        logging.info("Disable scale-in protection:", instances)
        set_asg_protection(asg_client, ASG, instances, is_enabled=False)

        logging.info("Force scale down ASG:", capacity_old)
        scale_asg(asg_client, capacity_old, ASG)

        # wait until desired != count of instances in ASG
        logging.info("Wait until old instances in ASG are terminated")
        iteration_timeout = 5
        while len(instances) != capacity_old['desired']:
            instances = get_asg_instances(asg_client, ASG)
            time.sleep(iteration_timeout)


if __name__ == '__main__':

    clients = auth()

    # generate k8s config
    gen_k8s_config(REGION, K8S_CLUSTER_NAME, K8S_ROLE)

    # create new Launch Template version with new AMI id
    create_launch_template_version(clients['ec2'], LT, AMI_ID, AMI_DESCRIPTION)

    # disable cluster autoscaler
    set_cluster_autoscaler(is_enabled=False)

    # taint k8 nodes
    k8s_old_nodes = get_k8s_nodes()
    taint_k8s_nodes(k8s_old_nodes)

    # scale up ASG (x2 max and desired capacity)
    capacity_old = get_asg_capacity(clients['asg'], ASG)
    capacity_double = {'desired': capacity_old['desired'] * 2,
                       'max': capacity_old['max'] * 2}
    scale_asg(clients['asg'], capacity_double, ASG)

    # wait until ASG instances will be InService status
    timeouts = {'iteration': 5, 'deadline': 300}
    cap_old = capacity_old['desired']
    cap_new = capacity_double['desired']
    wait_asg_status(clients['asg'], cap_old, cap_new, ASG, timeouts)

    # enable scale-in protection for all ASG instances
    instances_all = get_asg_instances(clients['asg'], ASG)
    instances_ids = [i['InstanceId'] for i in instances_all]
    set_asg_protection(clients['asg'], ASG, instances_ids)

    # wait until k8s nodes will be in Ready status
    timeouts = {'iteration': 5, 'deadline': 300}
    wait_k8s_nodes_status(capacity_double['desired'], timeouts)

    # wait 2 hours or until pods controlled by jobs are completed
    timeouts = {'deadline': 60 * 60 * 2, 'iteration': 60}
    replace_nodes(clients['asg'], K8S_JOBS_FILTERS, k8s_old_nodes, timeouts)

    # get back to the original MAX capacity
    scale_asg(clients['asg'], capacity_old, ASG)

    # disable scale-in protection for NEW instances
    instances_all = get_asg_instances(clients['asg'], ASG)
    instances_ids = [i['InstanceId'] for i in instances_all]
    set_asg_protection(clients['asg'], ASG, instances_ids, is_enabled=False)

    # enable cluster-autoscaler
    set_cluster_autoscaler(is_enabled=True)
