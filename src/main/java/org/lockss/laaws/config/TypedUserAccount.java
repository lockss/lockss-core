/*

Copyright (c) 2000-2023, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.laaws.config;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * Wrapper for a serialized user account
 */
@ApiModel(description = "Wrapper for a serialized user account")


public class TypedUserAccount   {
  @JsonProperty("userAccountType")
  private String userAccountType = null;

  @JsonProperty("serializedData")
  private String serializedData = null;

  public TypedUserAccount userAccountType(String userAccountType) {
    this.userAccountType = userAccountType;
    return this;
  }

  /**
   * Type of user account
   * @return userAccountType
  **/
  @ApiModelProperty(value = "Type of user account")


  public String getUserAccountType() {
    return userAccountType;
  }

  public void setUserAccountType(String userAccountType) {
    this.userAccountType = userAccountType;
  }

  public TypedUserAccount serializedData(String serializedData) {
    this.serializedData = serializedData;
    return this;
  }

  /**
   * Output from the user account serialization
   * @return serializedData
  **/
  @ApiModelProperty(value = "Output from the user account serialization")


  public String getSerializedData() {
    return serializedData;
  }

  public void setSerializedData(String serializedData) {
    this.serializedData = serializedData;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TypedUserAccount typedUserAccount = (TypedUserAccount) o;
    return Objects.equals(this.userAccountType, typedUserAccount.userAccountType) &&
        Objects.equals(this.serializedData, typedUserAccount.serializedData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userAccountType, serializedData);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class TypedUserAccount {\n");
    
    sb.append("    userAccountType: ").append(toIndentedString(userAccountType)).append("\n");
    sb.append("    serializedData: ").append(toIndentedString(serializedData)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

