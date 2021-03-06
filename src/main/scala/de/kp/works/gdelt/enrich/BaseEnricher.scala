package de.kp.works.gdelt.enrich
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */
import de.kp.works.gdelt.GDELTModel

trait BaseEnricher[T] {
  
  protected val cameoCountryCodes: Map[String, String] = GDELTModel.cameoCountryCodes
  protected val countryCodes: Map[String, String] = GDELTModel.countryCodes

  protected val eventCodes: Map[String, String] = GDELTModel.eventCodes
  protected val ethnicCodes: Map[String, String] = GDELTModel.ethnicCodes
  
  protected val groupCodes: Map[String, String] = GDELTModel.groupCodes
  protected val religionCodes: Map[String, String] = GDELTModel.religionCodes

  protected val typeCodes: Map[String, String] = GDELTModel.typeCodes

  protected var resolution:Int = 7
  protected var version:String = "V1"
   
  def setResolution(value:Int):T = {
    resolution = value
    this.asInstanceOf[T]
  }
 
  def setVersion(value:String):T = {
    version = value
    this.asInstanceOf[T]
  }

}