--
-- Copyright 2010-2016 Boxfuse GmbH
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--         http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--


CREATE FUNCTION F_SQUARE(X INT)
  RETURNS NUMERIC(20, 0)
LANGUAGE SQL RETURN X * X;


CREATE FUNCTION PROC_WITH_5_PARAMS(P1 INTEGER, P2 VARCHAR(5), P3 NUMERIC(20, 0), p4 TIMESTAMP, p5 DATE)
  RETURNS INTEGER
LANGUAGE SQL  RETURN P1;


