from interfaz_azure.key_vault import obtenerPasswordDesdeKeyVault
from interfaz_azure.variables import *
from azure.core.exceptions import ResourceExistsError

class BlobStorageInterface:
    def __init__(self, nombre_contenedor = "", cuenta_almacenamiento='', llave_cuenta_almacenamiento=''):
        self.nombre_contenedor = nombre_contenedor
        
        if (cuenta_almacenamiento == '') and (llave_cuenta_almacenamiento == ''):
            self.string_conexion_cuenta_almacenamiento = \
                obtenerPasswordDesdeKeyVault("string_conexion_cuenta_almacenamiento")
        else:
            self.string_conexion_cuenta_almacenamiento = (
                'DefaultEndpointsProtocol=https;' 
                + f'AccountName={cuenta_almacenamiento};' 
                + f'AccountKey={llave_cuenta_almacenamiento};' 
                + 'EndpointSuffix=core.windows.net'
            )
        

        self.cliente_blobAzure = None
        self.contenedor_azure = None
        self.establecerConexion()

    def establecerConexion(self):
        #https://www.quickprogrammingtips.com/azure/how-to-download-blobs-from-azure-storage-using-python.html
        
        from azure.storage.blob import BlobServiceClient
        #from azure.storage.blob import ContentSettings, ContainerClient
        print("Estableciendo conexion con servicio Blob")
        self.cliente_blobAzure = BlobServiceClient.from_connection_string(self.string_conexion_cuenta_almacenamiento)
        self.contenedor_azure = self.cliente_blobAzure.get_container_client(self.nombre_contenedor)
        print("Conexion establecida con servicio Blob")
        return

    def descargarCsv(self, archivo_csv = '', header=0, sep=",", encoding='utf-8'):
        from io import StringIO
        import pandas as pd
        if self.contenedor_azure is None:
            self.establecerConexion()
        
        string_csv = self.contenedor_azure.get_blob_client(archivo_csv).download_blob().readall()
        
        #liberar conexion con azure
        #self.contenedor_azure = None
        return pd.read_csv(StringIO(str(string_csv,encoding)), sep=sep, header=header)

    def crearContenedor(self, container_name):
        try:
            self.cliente_blobAzure.create_container(container_name)
        except ResourceExistsError:
            pass

    def escribir(self, df_salida, nombre_contenedor, nombre_archivo_salida = 'output/output.csv'):
        if self.cliente_blobAzure is None:
            self.establecerConexion()
        if df_salida is not None:
            print("Guardando informacion en csv")
            self.crearContenedor(nombre_contenedor)
            # Instantiate a new BlobClient
            blob_client = self.cliente_blobAzure.get_blob_client(
                container=nombre_contenedor,
                blob=nombre_archivo_salida
            )
            blob_client.upload_blob(df_salida.to_csv(index=False, encoding = "utf-8"), blob_type="BlockBlob",overwrite=True)
            #contenedor_azure.get_blob_client("myblockblob").upload_blob(df_salida, blob_type="BlockBlob")
        return


def main():
    blob = BlobStorageInterface()
    return

if __name__ == '__main__':

    main()



