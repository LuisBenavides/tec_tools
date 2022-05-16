import retencion.azure_interface.variables as az_var

class AMLInterface(object):
    def __init__(self):
        self.workspace = None
        self.environment = None
        self.ml_config = None
        self.establecerConexion()

    def establecerConexion(self):
        #global ML_WS, AML_INTERFACE
        from azureml.core import Workspace
        #from azure.identity import DefaultAzureCredential
        from azureml.core.authentication import ServicePrincipalAuthentication
        # Enter your workspace subscription, resource group, name, and region.
        from retencion.azure_interface.key_vault_interface import obtenerPasswordDesdeKeyVault
        svc_pr_password = obtenerPasswordDesdeKeyVault("app-III-IndPred-password")
        
        svc_pr = ServicePrincipalAuthentication(
            tenant_id="c65a3ea6-0f7c-400b-8934-5a6dc1705645",
            service_principal_id="b7334989-3def-45f8-acdf-f248b4ffc48b",
            service_principal_password=svc_pr_password
            )
        subscription_id = "8b8959ee-bc8c-4480-9126-4e7f2e1d0b20" #you should be owner or contributor
        resource_group = "rg_III_IndPred_poc" #you should be owner or contributor
        workspace_name = "ml_ws_III_IndPred" #your workspace name
        workspace_region = "South Central US" #your region
        if self.workspace is None:
            self.workspace = Workspace(workspace_name = workspace_name,
                    subscription_id = subscription_id,
                    resource_group = resource_group,
                    auth=svc_pr)

        print("Found workspace {} at location {}".format(
            self.workspace.name,
            self.workspace.location
            )
        )
        return

    def subirDatos(self, training_data):
        from azureml.core import Dataset
        
        # Get the Azure Machine Learning default datastore
        datastore = self.workspace.get_default_datastore()

        training_data.to_csv('data/output/training_pd.csv', index=False)

        # Convert into an Azure Machine Learning tabular dataset
        datastore.upload_files(files = ['data/output/training_pd.csv'],
                            target_path = 'train-dataset/tabular/',
                            overwrite = True,
                            show_progress = True)
        dataset_training = Dataset.Tabular.from_delimited_files(path = [(datastore, 'train-dataset/tabular/training_pd.csv')])
        return dataset_training

    def crearAmbiente(self):
        from azureml.core.environment import Environment
        if self.environment is None:
            self.environment = Environment.from_pip_requirements(
                name=az_var.AML_ENV_NAME,
                file_path='src/retencion/utils/requirements.txt'
                )
        self.environment.docker.enabled = True
        return

    def registrarAmbiente(self):
        self.environment.register(workspace=self.workspace)
        return

    def crearTrainPipeline(self, model_id=1):
        from azureml.core import RunConfiguration
        from retencion.utils.global_variables import METODO_PUNTO_CORTE
        from azureml.pipeline.steps import PythonScriptStep
        from pathlib import Path

        src_dir = Path("retencion").parent
        train_mdl_path = "src/retencion/train.py"#Path("train.py").relative_to(src_dir)
        compute_target = self.obtenerComputeTarget(
                                compute_name = az_var.AML_COMPUTE_NAME,
                                vm_size = 'STANDARD_D2_V2'
                            )
        train_step = (
            PythonScriptStep(
                name="train_model", 
                script_name=str(train_mdl_path),
                source_directory=src_dir, 
                runconfig=RunConfiguration(),
                arguments=[az_var.MODEL_NAME,
                            model_id,
                            az_var.stringConexionCuentaAlmacenamiento,
                            az_var.BLOL_CONTAINER_NAME,
                            METODO_PUNTO_CORTE
                            ],
                compute_target=compute_target,
                allow_reuse=True,
            )
        )
        return train_step

    def crearPipelinesCleanTrain(self):
        from azureml.core import ScriptRunConfig, RunConfiguration
        from retencion.utils.global_variables import METODO_PUNTO_CORTE
        from azureml.pipeline.steps import PythonScriptStep
        from pathlib import Path

        src_dir = Path(retencion.__file__)
        clean_mdl_path = Path(clean.__file__).relative_to(src_dir)
        train_mdl_path = Path(train.__file__).relative_to(src_dir)

        compute_target = self.obtenerComputeTarget(
                                compute_name = az_var.AML_COMPUTE_NAME,
                                vm_size = 'STANDARD_D2_V2'
                            )

        clean_step = (
            PythonScriptStep(
                name="clean data", 
                script_name=str(clean_mdl_path),
                source_directory=src_dir, 
                runconfig=RunConfiguration(),
                compute_target=compute_target,
                allow_reuse=True,
            )
        )
        train_step = (
            PythonScriptStep(
                name="train_model", 
                script_name=str(train_mdl_path),
                source_directory=src_dir, 
                runconfig=RunConfiguration(),
                arguments=[az_var.MODEL_NAME,
                            az_var.stringConexionCuentaAlmacenamiento,
                            az_var.BLOL_CONTAINER_NAME,
                            METODO_PUNTO_CORTE
                            ],
                compute_target=compute_target,
                allow_reuse=True,
            )
        )
        return clean_step, train_step
        # run a trial from the train.py code in your current directory
        ml_config = ScriptRunConfig(
                        source_directory='./src',
                        script='retencion/main/train.py',
                        arguments=[
                            az_var.MODEL_NAME,
                            az_var.stringConexionCuentaAlmacenamiento,
                            az_var.nombreContenedor,
                            metodo_punto_corte
                        ]
                    )
        ml_config.run_config.target = self.obtenerComputeTarget(
                                                compute_name = az_var.AML_COMPUTE_NAME,
                                                vm_size = 'STANDARD_D2_V2'
                                            )
        return

    def subirEjecucion(self):
        
        from azureml.core import Experiment, Environment
        from azureml.pipeline.core.pipeline import Pipeline

        experiment = Experiment(self.workspace, az_var.AML_EXPERIMENT_NAME)

        steps = list()

        max_models_id = 1
        for model_id in range(1, max_models_id+1):
            train_step = self.crearTrainPipeline(model_id=model_id)
            steps.append(train_step)

        if self.environment is None:
            self.environment = Environment.get(
                self.workspace,
                az_var.AML_ENV_NAME
            )

        print("Submitting Pipeline Steps")
        pipeline = Pipeline(self.workspace, steps=steps)
        run = pipeline.submit(experiment_name=experiment.name)
        run.wait_for_completion(raise_on_error=True)
        return
        print("Submitting Run")
        run = experiment.submit(config=self.ml_config)
        run.wait_for_completion(show_output=True)
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
            print('Found existing compute target')
        except ComputeTargetException:
            print('Creating a new compute target...')
            compute_config = AmlCompute.provisioning_configuration(
                vm_size=vm_size,
                min_nodes=1,
                max_nodes=2
            )
            compute_target = ComputeTarget.create(
                az_var.ML_WS,
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
            model = Model.get_model_path(model_name = az_var.MODEL_NAME, _workspace=self.workspace)
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

        src_dir = Path(retencion.__file__).parent.parent
        scoring_script_path = Path(score.__file__).relative_to(src_dir)

        if self.environment is None:
            self.environment = Environment.get(
                self.workspace,
                az_var.AML_ENV_NAME
            )

        inference_config = InferenceConfig(
            entry_script=scoring_script_path,
            environment=self.environment
        )
        return inference_config

    def desplegarServicio(self):
        from azureml.core.webservice import AciWebservice, Webservice
        from azureml.core.model import Model
        inference_config = self.obtenerConfiguracionInferencia()
        deployment_config = AciWebservice.deploy_configuration(
            cpu_cores=1,
            memory_gb=1
        )
        model = self.workspace.models.get(az_var.MODEL_NAME)
        service = Model.deploy(
            self.workspace,
            az_var.DEPLOYMENT_SERVICE_NAME,
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
            name=az_var.DEPLOYMENT_SERVICE_NAME,
            workspace=self.workspace
        )
        model = self.workspace.models.get(az_var.MODEL_NAME)
        service.update(models=[model], inference_config=inference_config)
        print(service.state)
        print(service.scoring_uri)
        return

    def desplegarModeloAML(self):
        webservices = self.workspace.webservices.keys()
        if az_var.DEPLOYMENT_SERVICE_NAME not in webservices:
            self.workspace.desplegarServicio()
        else:
            self.workspace.actualizarServicio()
        return

