// Copyright 2023 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.language.hostedService.api;

import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.finos.legend.engine.functionActivator.api.output.FunctionActivatorInfo;
import org.finos.legend.engine.functionActivator.validation.FunctionActivatorResult;
import org.finos.legend.engine.functionActivator.validation.FunctionActivatorValidator;
import org.finos.legend.engine.functionActivator.validation.FunctionActivatorWarning;
import org.finos.legend.engine.protocol.functionActivator.deployment.FunctionActivatorDeploymentConfiguration;
import org.finos.legend.engine.protocol.hostedService.deployment.HostedServiceArtifact;
import org.finos.legend.engine.protocol.hostedService.deployment.HostedServiceDeploymentConfiguration;
import org.finos.legend.engine.functionActivator.validation.FunctionActivatorError;
import org.finos.legend.engine.functionActivator.service.FunctionActivatorService;
import org.finos.legend.engine.language.hostedService.generation.deployment.HostedServiceDeploymentManager;
import org.finos.legend.engine.protocol.hostedService.deployment.HostedServiceDeploymentDetails;
import org.finos.legend.engine.protocol.hostedService.deployment.HostedServiceDeploymentResult;
import org.finos.legend.engine.language.hostedService.generation.HostedServiceArtifactGenerator;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.protocol.hostedService.metamodel.HostedServiceProtocolExtension;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContext;
import org.finos.legend.engine.shared.core.identity.Identity;
import org.finos.legend.pure.generated.*;

import java.util.List;

public class HostedServiceService implements FunctionActivatorService<Root_meta_external_function_activator_hostedService_HostedService, HostedServiceDeploymentConfiguration, HostedServiceDeploymentResult>
{
    private final HostedServiceArtifactGenerator hostedServiceArtifactgenerator;
    private final HostedServiceDeploymentManager hostedServiceDeploymentManager;
    private MutableList<FunctionActivatorValidator> extraValidators = Lists.mutable.empty();


    public HostedServiceService()
    {

        this.hostedServiceArtifactgenerator = new HostedServiceArtifactGenerator();
        this.hostedServiceDeploymentManager = new HostedServiceDeploymentManager();
    }

    public HostedServiceService(List<FunctionActivatorValidator> extraValidators)
    {
        this();
        this.extraValidators = Lists.mutable.withAll(extraValidators);
    }

    @Override
    public MutableList<String> group()
    {
        return org.eclipse.collections.impl.factory.Lists.mutable.with("Function_Activator", "Hosted_Service");
    }

    @Override
    public FunctionActivatorInfo info(PureModel pureModel, String version)
    {
        return new FunctionActivatorInfo(
                "Hosted Service",
                "Create a HostedService that will be deployed to a server environment and executed with a pattern",
                "meta::protocols::pure::" + version + "::metamodel::function::activator::hostedService::HostedService",
                HostedServiceProtocolExtension.packageJSONType,
                pureModel);
    }

    @Override
    public boolean supports(Root_meta_external_function_activator_FunctionActivator functionActivator)
    {
        return functionActivator instanceof Root_meta_external_function_activator_hostedService_HostedService;
    }

    @Override
    public FunctionActivatorResult validate(Identity identity, PureModel pureModel, Root_meta_external_function_activator_hostedService_HostedService activator, PureModelContext inputModel, List<HostedServiceDeploymentConfiguration> runtimeConfigurations, Function<PureModel, RichIterable<? extends Root_meta_pure_extension_Extension>> routerExtensions)
    {
        MutableList<HostedServiceError> errors =  Lists.mutable.empty();
        FunctionActivatorResult result = new FunctionActivatorResult();
        try
        {
            core_hostedservice_generation_generation.Root_meta_external_function_activator_hostedService_validator_validateService_HostedService_1__Boolean_1_(activator, pureModel.getExecutionSupport()); //returns true or errors out

        }
        catch (Exception e)
        {
           errors.add(new HostedServiceError("HostedService can't be registered.", e));
        }
        this.extraValidators.select(v -> v.supports(activator)).forEach(v ->
        {
            errors.addAll(v.validate(identity, activator));
        });
        result.addAll(validateArtifactActions(identity, pureModel, activator, inputModel, runtimeConfigurations, "vX_X_X", routerExtensions));
        result.getErrors().addAll(errors);
        return result;
    }

    public FunctionActivatorResult validateArtifactActions(Identity identity, PureModel pureModel, Root_meta_external_function_activator_hostedService_HostedService activator, PureModelContext inputModel, List<HostedServiceDeploymentConfiguration> availableRuntimeConfigurations, String clientVersion, Function<PureModel, RichIterable<? extends Root_meta_pure_extension_Extension>> routerExtensions)
    {
        FunctionActivatorResult result = new FunctionActivatorResult();
        if (!activator._actions().isEmpty())
        {
            HostedServiceDeploymentConfiguration deployConf;
            try
            {
                HostedServiceArtifact artifact = this.renderArtifact(pureModel, activator, inputModel, clientVersion, routerExtensions);
                deployConf = hostedServiceDeploymentManager.getDeploymentConfiguration(artifact, availableRuntimeConfigurations);
                if (artifact.version == null)
                {
                    artifact.deploymentConfiguration = deployConf;
                }

                HostedServiceDeploymentDetails serviceDetails = this.hostedServiceDeploymentManager.getActivatorDetails(identity, (HostedServiceDeploymentConfiguration) artifact.deploymentConfiguration, activator);

                if (serviceDetails.errorMessage != null)
                {
                    throw new Exception(serviceDetails.errorMessage);
                }

                this.extraValidators.select(v -> v.supports(activator)).forEach(v ->
                {
                    result.addAll(v.validateArtifactActions(artifact.actions, serviceDetails.actions));
                });
            }
            catch (Exception e)
            {
                result.addWarning(new FunctionActivatorWarning("Failed to validate Artifact Actions. Error: " + e.getMessage()));
            }
        }
        return result;
    }

    @Override
    public HostedServiceArtifact renderArtifact(PureModel pureModel, Root_meta_external_function_activator_hostedService_HostedService activator, PureModelContext inputModel, String clientVersion, Function<PureModel, RichIterable<? extends Root_meta_pure_extension_Extension>> routerExtensions)
    {
        return this.hostedServiceArtifactgenerator.renderServiceArtifact(pureModel, activator, inputModel, clientVersion, routerExtensions);
    }

    @Override
    public String generateLineage(PureModel pureModel, Root_meta_external_function_activator_hostedService_HostedService activator, PureModelContext inputModel, String clientVersion, Function<PureModel, RichIterable<? extends Root_meta_pure_extension_Extension>> routerExtensions)
    {
        return this.hostedServiceArtifactgenerator.generateLineage(pureModel, activator, inputModel, routerExtensions);
    }

    @Override
    public List<HostedServiceDeploymentConfiguration> selectConfig(List<FunctionActivatorDeploymentConfiguration> configurations)
    {
        return Lists.mutable.withAll(configurations).select(e -> e instanceof HostedServiceDeploymentConfiguration).collect(e -> (HostedServiceDeploymentConfiguration)e);
    }

    @Override
    public HostedServiceDeploymentResult publishToSandbox(Identity identity, PureModel pureModel, Root_meta_external_function_activator_hostedService_HostedService activator, PureModelContext inputModel, List<HostedServiceDeploymentConfiguration> runtimeConfigs, Function<PureModel, RichIterable<? extends Root_meta_pure_extension_Extension>> routerExtensions)
    {
        MutableList<? extends FunctionActivatorError> validationErrors = this.validate(identity, pureModel, activator, inputModel, runtimeConfigs, routerExtensions).getErrors();
        if (validationErrors.isEmpty())
        {
            HostedServiceArtifact artifact = this.hostedServiceArtifactgenerator.renderServiceArtifact(pureModel, activator, inputModel, "vX_X_X", routerExtensions);
            HostedServiceDeploymentResult result = this.hostedServiceDeploymentManager.deploy(identity, artifact, runtimeConfigs);
            if (result.successful)
            {
                HostedServiceDeploymentConfiguration deployConf = hostedServiceDeploymentManager.getDeploymentConfiguration(artifact, runtimeConfigs);
                if (artifact.version == null)
                {
                    artifact.deploymentConfiguration = deployConf;
                }
                result.actionResults = this.hostedServiceDeploymentManager.deployActions(identity, artifact);
            }
            return result;
        }
        return new HostedServiceDeploymentResult(validationErrors.collect(v -> v.message));
    }

}
