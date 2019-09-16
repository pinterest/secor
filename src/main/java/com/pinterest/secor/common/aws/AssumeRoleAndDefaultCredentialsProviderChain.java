package com.pinterest.secor.common.aws;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.internal.*;
import com.amazonaws.auth.profile.internal.securitytoken.STSProfileCredentialsServiceLoader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.amazonaws.profile.path.AwsProfileFileLocationProvider.DEFAULT_CONFIG_LOCATION_PROVIDER;
import static com.amazonaws.profile.path.AwsProfileFileLocationProvider.DEFAULT_CREDENTIALS_LOCATION_PROVIDER;

public class AssumeRoleAndDefaultCredentialsProviderChain extends AWSCredentialsProviderChain {

  private static final Log log = LogFactory.getLog(AssumeRoleAndDefaultCredentialsProviderChain.class);

  private static final AssumeRoleAndDefaultCredentialsProviderChain INSTANCE
    = new AssumeRoleAndDefaultCredentialsProviderChain();

  public AssumeRoleAndDefaultCredentialsProviderChain() {
    super(getDefaultCredentials(),
      new SystemPropertiesCredentialsProvider(),
      new ProfileCredentialsProvider(),
      new EC2ContainerCredentialsProviderWrapper(),
      WebIdentityTokenCredentialsProvider.builder().build());
  }

  public static AssumeRoleAndDefaultCredentialsProviderChain getInstance() {
    return INSTANCE;
  }

  private static AWSCredentialsProvider getDefaultCredentials() {
    log.info("Using AssumeRoleAndDefaultCredentialsProviderChain");
    final String profileName = "profile " + AwsProfileNameLoader.INSTANCE.loadProfileName();

    final AllProfiles allProfiles = new AllProfiles(Stream
      .concat(BasicProfileConfigLoader.INSTANCE
          .loadProfiles(DEFAULT_CONFIG_LOCATION_PROVIDER.getLocation())
          .getProfiles()
          .values().stream(),
        BasicProfileConfigLoader.INSTANCE
          .loadProfiles(DEFAULT_CREDENTIALS_LOCATION_PROVIDER.getLocation())
          .getProfiles().values().stream())
      .map(profile -> new BasicProfile(profile.getProfileName(), profile.getProperties()))
      .collect(Collectors
        .toMap(BasicProfile::getProfileName,
          profile -> profile,
          (left, right) -> {
            final Map<String, String> properties = new HashMap<>(left.getProperties());
            properties.putAll(right.getProperties());
            return new BasicProfile(left.getProfileName(), properties);
          })));

    final BasicProfile profile = Optional.ofNullable(allProfiles.getProfile(profileName))
                                         .orElseThrow(() -> new RuntimeException(String.format("Profile '%s' not found in %s",
                                           profileName, allProfiles.getProfiles().keySet())));

    log.info("With profile : " + profileName);

    if (profile.isRoleBasedProfile()) {
      return new ProfileAssumeRoleCredentialsProvider(STSProfileCredentialsServiceLoader.getInstance(), allProfiles, profile);
    } else {
      return new ProfileStaticCredentialsProvider(profile);
    }
  }
}
