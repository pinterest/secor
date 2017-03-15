#!/usr/bin/env bash

########################################################################################################################
#
# Copyright © 2017 Unified Social, Inc.
# 180 Madison Avenue, 23rd Floor, New York, NY 10016, U.S.A.
# All rights reserved.
#
# This software (the "Software") is provided pursuant to the license agreement you entered into with Unified Social,
# Inc. (the "License Agreement").  The Software is the confidential and proprietary information of Unified Social,
# Inc., and you shall use it only in accordance with the terms and conditions of the License Agreement.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND "AS AVAILABLE."  UNIFIED SOCIAL, INC. MAKES NO WARRANTIES OF ANY KIND, WHETHER
# EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO THE IMPLIED WARRANTIES AND CONDITIONS OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT.
#
########################################################################################################################


# ======================================================================================================================
# CONFIGURATION
# ======================================================================================================================

# ----------  Global configuration  ----------

[ -z "${ENVIRONMENT_TYPE}" ] && declare ENVIRONMENT_TYPE="production"


# ----------  Jenkins  ----------

[ -z "${BASE_REMOTE_JENKINS}" ] && declare BASE_REMOTE_JENKINS=`dirname $0`"/../../../../../Base/src/main/scripts/build"

source "${BASE_REMOTE_JENKINS}/jenkins.sh"


# ----------  Local configuration  ----------

declare -a JOBS=(                   \
    Secor_01_Package	            \
    Secor_03_Deploy	                \
    Secor_04_Manage_Service         \
)


# ======================================================================================================================
# MAIN
# ======================================================================================================================

main_loop "$1" "Secor" JOBS "$2"


# ======================================================================================================================
