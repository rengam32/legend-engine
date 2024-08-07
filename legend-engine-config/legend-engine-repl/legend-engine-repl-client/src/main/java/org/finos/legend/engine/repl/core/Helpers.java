// Copyright 2024 Goldman Sachs
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

package org.finos.legend.engine.repl.core;

import org.finos.legend.engine.repl.client.Client;
import org.finos.legend.engine.shared.core.identity.Identity;
import org.finos.legend.engine.shared.core.kerberos.SubjectTools;

public class Helpers
{
    public static final String REPL_RUN_FUNCTION_QUALIFIED_PATH = "repl::__internal__::run__Any_MANY_";
    public static final String REPL_RUN_FUNCTION_SIGNATURE = "repl::__internal__::run():Any[*]";

    public static Identity resolveIdentityFromLocalSubject(Client client)
    {
        try
        {
            return Identity.makeIdentity(SubjectTools.getLocalSubject());
        }
        catch (Exception e)
        {
            if (client.isDebug())
            {
                client.getTerminal().writer().println("Couldn't resolve identity from local subject");
            }
            return Identity.getAnonymousIdentity();
        }
    }
}
