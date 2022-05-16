from interfaz_azure.key_vault import obtenerPasswordDesdeKeyVault
from interfaz_azure.variables import *
from azure.core.exceptions import ResourceExistsError

class BlobStorageInterface:
    def __init__(self, cuentaAlmacenamiento='', llaveCuentaAlmacenamiento=''):
        self.nombreContenedor = 'iii-indpred-ml'
        
        if (cuentaAlmacenamiento == '') and (llaveCuentaAlmacenamiento == ''):
            self.stringConexionCuentaAlmacenamiento = \
                obtenerPasswordDesdeKeyVault("stringConexionCuentaAlmacenamiento")
        else:
            self.stringConexionCuentaAlmacenamiento = (
                'DefaultEndpointsProtocol=https;' 
                + f'AccountName={cuentaAlmacenamiento};' 
                + f'AccountKey={llaveCuentaAlmacenamiento};' 
                + 'EndpointSuffix=core.windows.net'
            )
        

        self.clienteBlobAzure = None
        self.contenedorAzure = None
        self.establecerConexion()

    def establecerConexion(self):
        #https://www.quickprogrammingtips.com/azure/how-to-download-blobs-from-azure-storage-using-python.html
        
        from azure.storage.blob import BlobServiceClient
        #from azure.storage.blob import ContentSettings, ContainerClient
        print("Estableciendo conexion con servicio Blob")
        self.clienteBlobAzure = BlobServiceClient.from_connection_string(self.stringConexionCuentaAlmacenamiento)
        self.contenedorAzure = self.clienteBlobAzure.get_container_client(self.nombreContenedor)
        print("Conexion establecida con servicio Blob")
        return

    def descargarCsv(self, archivo_csv = '', header=0):
        from io import StringIO
        import pandas as pd
        if self.contenedorAzure is None:
            self.establecerConexion()
        
        string_csv = self.contenedorAzure.get_blob_client(archivo_csv).download_blob().readall()
        
        #liberar conexion con azure
        #self.contenedorAzure = None
        return pd.read_csv(StringIO(str(string_csv,'utf-8')), sep=",", header=header)

    def crearContenedor(self, container_name):
        try:
            self.clienteBlobAzure.create_container(container_name)
        except ResourceExistsError:
            pass

    def escribir(self, df_salida, nombre_contenedor, nombreArchivoSalida = 'output/output.csv'):
        if self.clienteBlobAzure is None:
            self.establecerConexion()
        if df_salida is not None:
            print("Guardando informacion en csv")
            self.crearContenedor(nombre_contenedor)
            # Instantiate a new BlobClient
            blob_client = self.clienteBlobAzure.get_blob_client(
                container=nombre_contenedor,
                blob=nombreArchivoSalida
            )
            blob_client.upload_blob(df_salida.to_csv(index=False, encoding = "utf-8"), blob_type="BlockBlob",overwrite=True)
            #contenedorAzure.get_blob_client("myblockblob").upload_blob(df_salida, blob_type="BlockBlob")
        return


def main():
    blob = BlobStorageInterface()
    return

if __name__ == '__main__':

    main()



