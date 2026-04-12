"""
Client WebHDFS pour interagir avec le cluster HDFS via l'API REST.
Documentation : https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
"""
import requests
import logging

logger = logging.getLogger(__name__)

WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"


class WebHDFSClient:
    """Client leger pour l'API WebHDFS d'Apache Hadoop."""

    def __init__(self, base_url: str = WEBHDFS_BASE_URL, user: str = WEBHDFS_USER):
        self.base_url = base_url
        self.user = user

    def _url(self, path: str, op: str, **params) -> str:
        """Construit l'URL WebHDFS pour une operation donnee."""
        # On construit l'URL avec le chemin, l'operation et le user
        url = f"{self.base_url}{path}?op={op}&user.name={self.user}"
        # On ajoute les parametres supplementaires (ex: overwrite=true)
        for key, value in params.items():
            url += f"&{key}={value}"
        return url

    def mkdirs(self, hdfs_path: str) -> bool:
        """
        Cree un repertoire (et ses parents) dans HDFS.
        Retourne True si succes, leve une exception sinon.
        """
        url = self._url(hdfs_path, "MKDIRS")
        response = requests.put(url)
        response.raise_for_status()
        result = response.json()
        if result.get("boolean"):
            logger.info("Repertoire HDFS cree : %s", hdfs_path)
            return True
        else:
            raise Exception(f"Echec creation repertoire HDFS : {hdfs_path}")

    def upload(self, hdfs_path: str, local_file_path: str) -> str:
        """
        Uploade un fichier local vers HDFS.
        Retourne le chemin HDFS du fichier uploade.

        WebHDFS upload = 2 etapes :
        1. PUT sur le NameNode (allow_redirects=False) -> recupere l'URL de redirection
        2. PUT sur le DataNode avec le contenu binaire du fichier
        """
        # Etape 1 : initier l'upload sur le NameNode
        url = self._url(hdfs_path, "CREATE", overwrite="true")
        response = requests.put(url, allow_redirects=False)

        if response.status_code != 307:
            raise Exception(
                f"Etape 1 upload echouee (attendu 307, recu {response.status_code})"
            )

        # Recuperer l'URL de redirection vers le DataNode
        redirect_url = response.headers["Location"]
        logger.info("Redirection vers DataNode : %s", redirect_url)

        # Etape 2 : envoyer le fichier vers le DataNode
        with open(local_file_path, "rb") as f:
            response2 = requests.put(
                redirect_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
            )

        if response2.status_code != 201:
            raise Exception(
                f"Etape 2 upload echouee (attendu 201, recu {response2.status_code})"
            )

        logger.info("Fichier uploade dans HDFS : %s", hdfs_path)
        return hdfs_path

    def open(self, hdfs_path: str) -> bytes:
        """
        Lit le contenu d'un fichier HDFS.
        Retourne les donnees brutes (bytes).
        """
        url = self._url(hdfs_path, "OPEN")
        # allow_redirects=True : WebHDFS redirige vers le DataNode
        response = requests.get(url, allow_redirects=True)
        response.raise_for_status()
        logger.info("Fichier HDFS lu : %s (%d octets)", hdfs_path, len(response.content))
        return response.content

    def exists(self, hdfs_path: str) -> bool:
        """Verifie si un fichier ou repertoire existe dans HDFS."""
        url = self._url(hdfs_path, "GETFILESTATUS")
        response = requests.get(url)
        # HTTP 200 = existe, HTTP 404 = n'existe pas
        return response.status_code == 200

    def list_status(self, hdfs_path: str) -> list:
        """Liste le contenu d'un repertoire HDFS."""
        url = self._url(hdfs_path, "LISTSTATUS")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("FileStatuses", {}).get("FileStatus", [])
