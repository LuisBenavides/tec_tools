import interfaz_azure.variables as variable_azure
from azureml.core import Datastore

class AMLInterface(object):
    def __init__(self, credenciales_service_principal, id_subscripcion,
                 nombre_aml_workspace, nombre_grupo_recursos):
        self.workspace = None
        self.environment = None
        self.ml_config = None
        self.establecerConexion(credenciales_service_principal, id_subscripcion,
                 nombre_aml_workspace, nombre_grupo_recursos)

    def establecerConexion(self, credenciales_service_principal, id_subscripcion,
                 nombre_aml_workspace, nombre_grupo_recursos):
        from azureml.core import Workspace
        from azureml.core.authentication import ServicePrincipalAuthentication
                
        svc_pr = ServicePrincipalAuthentication(
            **credenciales_service_principal
            )
        id_subscripcion = "8b8959ee-bc8c-4480-9126-4e7f2e1d0b20" #you should be owner or contributor
        nombre_grupo_recursos = "rg_III_IndPred_poc" #you should be owner or contributor
        nombre_aml_workspace = "ml_ws_III_IndPred" #your workspace name
        #workspace_region = "South Central US" #your region
        if self.workspace is None:
            self.workspace = Workspace(workspace_name= nombre_aml_workspace,
                    subscription_id = id_subscripcion,
                    resource_group = nombre_grupo_recursos,
                    auth=svc_pr)

        print("Worskpace encontrado {} en ubicación {}".format(
            self.workspace.name,
            self.workspace.location
            )
        )
        return

    def subirDatos(self, df_training_data):
        from azureml.core import Dataset
        
        Dataset.Tabular.register_pandas_dataframe(dataframe=df_training_data, target='train-dataset/tabular/', name="trainin_data")

        return 

    def registrarDatastore(self, nombre_datastore, nombre_contenedor_blob,
                           nombre_cuenta_almacenamiento, llave_cuenta_almacenamiento):
        Datastore.register_azure_blob_container(
            workspace=self.workspace,
            datastore_name=nombre_datastore,
            container_name=nombre_contenedor_blob,
            account_name=nombre_cuenta_almacenamiento,
            account_key=llave_cuenta_almacenamiento
        )

    def crearAmbiente(self):
        from azureml.core.environment import Environment
 
        self.environment = Environment.from_pip_requirements(
            name=variable_azure.AML_ENV_NAME,
            file_path='src/alertas_tempranas/utilidades/requirements.txt'
            )
 
        return

    def registrarAmbiente(self):
        self.environment.register(workspace=self.workspace)
        return

    def configuracionScriptEntrenamiento(self):
        from azureml.core import ScriptRunConfig, Environment
        from alertas_tempranas.utilidades.variables_globales import METODO_PUNTO_CORTE
        from pathlib import Path
        from alertas_tempranas import train
        import alertas_tempranas
        # run a trial from the train.py code in your current directory

        src_dir = Path(alertas_tempranas.__file__).parent.parent
        train_mdl_path = Path(train.__file__).relative_to(src_dir)

        self.ml_config = ScriptRunConfig(
                        source_directory=str(src_dir),
                        script=str(train_mdl_path),
                        arguments=[
                            variable_azure.MODEL_NAME,
                            variable_azure.stringConexionCuentaAlmacenamiento,
                            variable_azure.BLOB_CONTAINER_NAME,
                            METODO_PUNTO_CORTE
                        ]
                    )
        self.ml_config.run_config.target = self.obtenerComputeTarget(
                                                compute_name = variable_azure.AML_COMPUTE_NAME,
                                                vm_size = 'STANDARD_D2_V2'
                                            )
        
        aml_run_env = Environment.get(
            self.workspace,
            variable_azure.AML_ENV_NAME
        )
        self.ml_config.run_config.environment = aml_run_env
        return

    def subirEjecucion(self):
        from azureml.core import Experiment

        experiment = Experiment(self.workspace, variable_azure.AML_EXPERIMENT_NAME)

        #for model_id in range(1, max_models_id+1):
        print("Iniciando Script Entrenamiento")
        self.configuracionScriptEntrenamiento()
        run = experiment.submit(config=self.ml_config)
        run.wait_for_completion(show_output=True)
        print("Entrenamiento Completado. Metricas obtenidas")
        print(run.get_metrics())
        return

    def obtenerComputeTarget(self, compute_name, vm_size=None):
        from azureml.core.compute import ComputeTarget, AmlCompute
        from azureml.exceptions import ComputeTargetException

        try:
            compute_target = ComputeTarget(
                workspace=self.workspace,
                name=compute_name
            )
            print('Compute target encontrado.')
        except ComputeTargetException:
            print('Creando un nuevo compute target...')
            compute_config = AmlCompute.provisioning_configuration(
                vm_size=vm_size,
                min_nodes=1,
                max_nodes=2
            )
            compute_target = ComputeTarget.create(
                variable_azure.ML_WS,
                compute_name,
                compute_config
            )
            compute_target.wait_for_completion(
                show_output=True,
                timeout_in_minutes=20
            )
        return compute_target

    def obtenerModelo(self):
        from azureml.core.model import Model
        import joblib

        try:
            model = Model.get_model_path(model_name = variable_azure.MODEL_NAME, _workspace=self.workspace)
            print(model)
            #model.download(target_dir=os.getcwd())
        except:
            print("Modelo no encontrado")
            return
        return joblib.load(model)

    def obtenerConfiguracionInferencia(self):
        from azureml.core.environment import Environment
        from azureml.core.model import InferenceConfig, Model
        from pathlib import Path
        from alertas_tempranas import score
        import alertas_tempranas
        src_dir = Path(alertas_tempranas.__file__).parent.parent
        scoring_script_path = Path(score.__file__).relative_to(src_dir)

        if self.environment is None:
            self.environment = Environment.get(
                self.workspace,
                variable_azure.AML_ENV_NAME
            )

        inference_config = InferenceConfig(
            entry_script=scoring_script_path,
            environment=self.environment
        )
        return inference_config

    def desplegarServicio(self):
        from azureml.core.webservice import AciWebservice
        from azureml.core.model import Model
        inference_config = self.obtenerConfiguracionInferencia()
        deployment_config = AciWebservice.deploy_configuration(
            cpu_cores=1,
            memory_gb=1
        )
        model = self.workspace.models.get(variable_azure.MODEL_NAME)
        service = Model.deploy(
            self.workspace,
            variable_azure.DEPLOYMENT_SERVICE_NAME,
            [model],
            inference_config,
            deployment_config)
        service.wait_for_deployment(show_output=True)
        print(service.scoring_uri)
        return

    def actualizarServicio(self):
        from azureml.core.webservice import Webservice

        inference_config = self.workspace.obtenerConfiguracionInferencia()
        service = Webservice(
            name=variable_azure.DEPLOYMENT_SERVICE_NAME,
            workspace=self.workspace
        )
        model = self.workspace.models.get(variable_azure.MODEL_NAME)
        service.update(models=[model], inference_config=inference_config)
        print(service.state)
        print(service.scoring_uri)
        return

    def desplegarModeloAML(self):
        webservices = self.workspace.webservices.keys()
        if variable_azure.DEPLOYMENT_SERVICE_NAME not in webservices:
            self.workspace.desplegarServicio()
        else:
            self.workspace.actualizarServicio()
        return

if __name__ == '__main__':
    from pathlib import Path
    from alertas_tempranas import train
    import alertas_tempranas
    # run a trial from the train.py code in your current directory

    src_dir = Path(alertas_tempranas.__file__).parent.parent
    train_mdl_path = Path(train.__file__).relative_to(src_dir)
    print(src_dir)
    print(train_mdl_path)
    