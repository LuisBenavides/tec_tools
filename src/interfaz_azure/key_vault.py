def obtenerPasswordDesdeKeyVault(nombre_pass):
    from azure.identity import ManagedIdentityCredential, DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    keyVaultName = "kv-iii-indpred"#os.environ["keyvault-chbot-telemetry"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    credential = DefaultAzureCredential()#ManagedIdentityCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)

    return client.get_secret(nombre_pass).value
