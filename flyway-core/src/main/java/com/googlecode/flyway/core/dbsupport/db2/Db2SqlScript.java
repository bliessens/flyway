/**
 * Copyright (C) 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.googlecode.flyway.core.dbsupport.db2;

import com.googlecode.flyway.core.migration.sql.PlaceholderReplacer;
import com.googlecode.flyway.core.migration.sql.SqlScript;

/**
 * SqlScript supporting DB2-specific delimiter changes.
 * 
 * TODO Support for Procedures.
 */
public class Db2SqlScript extends SqlScript
{

	/**
	 * Creates a new sql script from this source with these placeholders to replace.
	 * 
	 * @param sqlScriptSource
	 *            The sql script as a text block with all placeholders still present.
	 * @param placeholderReplacer
	 *            The placeholder replacer to apply to sql migration scripts.
	 * 
	 * @throws IllegalStateException
	 *             Thrown when the script could not be read from this resource.
	 */
	public Db2SqlScript(String sqlScriptSource, PlaceholderReplacer placeholderReplacer)
	{
		super(sqlScriptSource, placeholderReplacer);
	}

}
