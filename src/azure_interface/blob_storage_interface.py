from azure_interface.variables import * 

class BlobStorageInterface:
    def __init__(self, nombreContenedor=None, stringConexionCuentaAlmacenamiento=None):
        self.nombreContenedor = nombreContenedor
        self.stringConexionCuentaAlmacenamiento = stringConexionCuentaAlmacenamiento
        self.clienteBlobAzure = None
        self.contenedorAzure = None
        self.establecerConexion()

    def establecerConexion(self):
        from azure_interface.key_vault_interface import obtenerPasswordDesdeKeyVault
        #https://www.quickprogrammingtips.com/azure/how-to-download-blobs-from-azure-storage-using-python.html
        
        from azure.storage.blob import BlobServiceClient
        #from azure.storage.blob import ContentSettings, ContainerClient
        contenedorAzure = None
        clienteBlobAzure = None
        if self.nombreContenedor is None and self.stringConexionCuentaAlmacenamiento is None:
            self.stringConexionCuentaAlmacenamiento = \
                obtenerPasswordDesdeKeyVault("stringConexionCuentaAlmacenamiento")
            self.nombreContenedor = BLOL_CONTAINER_NAME

            if self.clienteBlobAzure is None or self.contenedorAzure is None:
                print("Estableciendo conexion con servicio Blob")
                self.clienteBlobAzure = BlobServiceClient.from_connection_string(self.stringConexionCuentaAlmacenamiento)
                self.contenedorAzure = self.clienteBlobAzure.get_container_client(self.nombreContenedor)
                print("Conexion establecida con servicio Blob")
        else:
            self.clienteBlobAzure = BlobServiceClient.from_connection_string(self.stringConexionCuentaAlmacenamiento)
            self.contenedorAzure = self.clienteBlobAzure.get_container_client(self.nombreContenedor)
        return

    def descargarCsv(self, archivo_csv = '', header=0):
        from io import StringIO
        import pandas as pd
        if self.contenedorAzure is None:
            self.establecerConexion()
        
        string_csv = self.contenedorAzure.get_blob_client(archivo_csv).download_blob().readall()
        
        #liberar conexion con azure
        self.contenedorAzure = None
        return pd.read_csv(StringIO(str(string_csv,'utf-8')), sep=",", header=header)


    def escribir(self, df_salida, nombreArchivoSalida = 'output/output.csv'):
        if df_salida is not None:
            print("Guardando informacion en csv")
            # Instantiate a new BlobClient
            blob_client = self.contenedorAzure.get_blob_client(nombreArchivoSalida)
            blob_client.upload_blob(df_salida.to_csv(index=False, encoding = "utf-8"), blob_type="BlockBlob",overwrite=True)
            #contenedorAzure.get_blob_client("myblockblob").upload_blob(df_salida, blob_type="BlockBlob")
        return






