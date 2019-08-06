import docker
from ..spark.constants import Constants

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

class DockerClient():
    def startContainer(self, containerName):
        client = docker.DockerClient(base_url= Constants.DOCKERCLIENT_BASE_URL,timeout = Constants.DOCKERCLIENT_TIMEOUT)
        container = client.containers.get(containerName)
        container.start()
        logger.info(' {} docker started'.format(containerName))
        return container