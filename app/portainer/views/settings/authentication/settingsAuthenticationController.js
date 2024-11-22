import angular from 'angular';
import _ from 'lodash-es';

import { buildLdapSettingsModel, buildAdSettingsModel } from '@/portainer/settings/authentication/ldap/ldap-settings.model';
import { options } from '@/react/portainer/settings/AuthenticationView/InternalAuth/options';
import { SERVER_TYPES } from '@/react/portainer/settings/AuthenticationView/ldap-options';
import { AuthenticationMethod } from '@/react/portainer/settings/types';

angular.module('portainer.app').controller('SettingsAuthenticationController', SettingsAuthenticationController);

function SettingsAuthenticationController($q, $scope, $state, Notifications, SettingsService, FileUploadService, TeamService, LDAPService) {
  $scope.authMethod = 1;

  $scope.state = {
    uploadInProgress: false,
    actionInProgress: false,
    availableUserSessionTimeoutOptions: [
      {
        key: '1 hour',
        value: '1h',
      },
      {
        key: '4 hours',
        value: '4h',
      },
      {
        key: '8 hours',
        value: '8h',
      },
      {
        key: '24 hours',
        value: '24h',
      },
      { key: '1 week', value: `${24 * 7}h` },
      { key: '1 month', value: `${24 * 30}h` },
      { key: '6 months', value: `${24 * 30 * 6}h` },
      { key: '1 year', value: `${24 * 30 * 12}h` },
    ],
  };

  $scope.formValues = {
    UserSessionTimeout: $scope.state.availableUserSessionTimeoutOptions[0],
    TLSCACert: '',
    ldap: {
      serverType: 0,
      adSettings: buildAdSettingsModel(),
      ldapSettings: buildLdapSettingsModel(),
    },
  };

  $scope.authOptions = options;

  $scope.onChangeAuthMethod = function onChangeAuthMethod(value) {
    $scope.authMethod = value;

    if (value === 4) {
      $scope.settings.AuthenticationMethod = AuthenticationMethod.LDAP;
      $scope.formValues.ldap.serverType = SERVER_TYPES.AD;
      return;
    }

    if (value === 2) {
      $scope.settings.AuthenticationMethod = AuthenticationMethod.LDAP;
      $scope.formValues.ldap.serverType = $scope.formValues.ldap.ldapSettings.ServerType;
      return;
    }

    $scope.settings.AuthenticationMethod = value;
  };

  $scope.onChangePasswordLength = function onChangePasswordLength(value) {
    $scope.$evalAsync(() => {
      $scope.settings.InternalAuthSettings = { RequiredPasswordLength: value };
    });
  };

  $scope.authenticationMethodSelected = function authenticationMethodSelected(value) {
    if (!$scope.settings) {
      return false;
    }

    if (value === AuthenticationMethod.AD) {
      return $scope.settings.AuthenticationMethod === AuthenticationMethod.LDAP && $scope.formValues.ldap.serverType === SERVER_TYPES.AD;
    }

    if (value === AuthenticationMethod.LDAP) {
      return $scope.settings.AuthenticationMethod === AuthenticationMethod.LDAP && $scope.formValues.ldap.serverType !== SERVER_TYPES.AD;
    }

    return $scope.settings.AuthenticationMethod === value;
  };

  $scope.isOauthEnabled = function isOauthEnabled() {
    return $scope.settings && $scope.settings.AuthenticationMethod === AuthenticationMethod.OAuth;
  };

  $scope.LDAPConnectivityCheck = LDAPConnectivityCheck;
  function LDAPConnectivityCheck() {
    const settings = angular.copy($scope.settings);

    const { settings: ldapSettings, uploadRequired, tlscaFile } = prepareLDAPSettings();
    settings.LDAPSettings = ldapSettings;
    $scope.state.uploadInProgress = uploadRequired;

    $scope.state.connectivityCheckInProgress = true;

    $q.when(!uploadRequired || FileUploadService.uploadLDAPTLSFiles(tlscaFile, null, null))
      .then(function success() {
        return LDAPService.check(settings.LDAPSettings);
      })
      .then(function success() {
        $scope.state.failedConnectivityCheck = false;
        $scope.state.successfulConnectivityCheck = true;
        Notifications.success('Success', 'Connection to LDAP successful');
      })
      .catch(function error(err) {
        $scope.state.failedConnectivityCheck = true;
        $scope.state.successfulConnectivityCheck = false;
        Notifications.error('Failure', err, 'Connection to LDAP failed');
      })
      .finally(function final() {
        $scope.state.uploadInProgress = false;
        $scope.state.connectivityCheckInProgress = false;
      });
  }

  $scope.saveSettings = function () {
    const settings = angular.copy($scope.settings);

    const { settings: ldapSettings, uploadRequired, tlscaFile } = prepareLDAPSettings();
    settings.LDAPSettings = ldapSettings;
    $scope.state.uploadInProgress = uploadRequired;

    $scope.state.actionInProgress = true;

    $q.when(!uploadRequired || FileUploadService.uploadLDAPTLSFiles(tlscaFile, null, null))
      .then(function success() {
        return SettingsService.update(settings);
      })
      .then(function success() {
        Notifications.success('Success', 'Authentication settings updated');
      })
      .catch(function error(err) {
        Notifications.error('Failure', err, 'Unable to update authentication settings');
      })
      .finally(function final() {
        $scope.state.uploadInProgress = false;
        $scope.state.actionInProgress = false;
      });
  };

  function prepareLDAPSettings() {
    const tlscaCert = $scope.formValues.TLSCACert;

    const tlscaFile = tlscaCert !== $scope.settings.LDAPSettings.TLSConfig.TLSCACert ? tlscaCert : null;

    const isADServer = $scope.formValues.ldap.serverType === SERVER_TYPES.AD;

    const settings = isADServer ? $scope.formValues.ldap.adSettings : $scope.formValues.ldap.ldapSettings;

    if (settings.AnonymousMode && !isADServer) {
      settings.ReaderDN = '';
      settings.Password = '';
    }

    if (isADServer) {
      settings.AnonymousMode = false;
    }

    settings.URLs = settings.URLs.map((url) => {
      if (url === undefined || url === '') {
        return;
      }

      if (url.includes(':')) {
        return url;
      }
      return url + (settings.TLSConfig.TLS ? ':636' : ':389');
    });

    const uploadRequired = (settings.TLSConfig.TLS || settings.StartTLS) && !settings.TLSConfig.TLSSkipVerify;

    settings.URL = settings.URLs[0];

    return { settings, uploadRequired, tlscaFile };
  }

  $scope.isLDAPFormValid = isLDAPFormValid;
  function isLDAPFormValid() {
    const ldapSettings = $scope.formValues.ldap.serverType === SERVER_TYPES.AD ? $scope.formValues.ldap.adSettings : $scope.formValues.ldap.ldapSettings;
    const isTLSMode = ldapSettings.TLSConfig.TLS || ldapSettings.StartTLS;

    return (
      _.compact(ldapSettings.URLs).length &&
      (ldapSettings.AnonymousMode || (ldapSettings.ReaderDN && isLDAPPasswordValid(ldapSettings.Password))) &&
      (!isTLSMode || (isTLSMode && $scope.formValues.TLSCACert) || ldapSettings.TLSConfig.TLSSkipVerify) &&
      (!$scope.settings.LDAPSettings.AdminAutoPopulate || ($scope.settings.LDAPSettings.AdminAutoPopulate && $scope.formValues.selectedAdminGroups.length > 0))
    );
  }

  // isLDAPPasswordValid is used to validate the password field in the LDAP settings form, and avoids the password field to be required when the form is in edit mode
  function isLDAPPasswordValid(password) {
    // if isEdit and it doesn't switch between AD and LDAP, then an empty password is valid
    if ($scope.state.isEditLDAP && $scope.state.initialServerType === $scope.formValues.ldap.serverType) {
      return true;
    }
    // otherwise the password is required
    return !!password;
  }

  $scope.isOAuthTeamMembershipFormValid = isOAuthTeamMembershipFormValid;
  function isOAuthTeamMembershipFormValid() {
    if ($scope.settings && $scope.settings.OAuthSettings.OAuthAutoMapTeamMemberships && $scope.settings.OAuthSettings.TeamMemberships) {
      if (!$scope.settings.OAuthSettings.TeamMemberships.OAuthClaimName) {
        return false;
      }

      const hasInvalidMapping = $scope.settings.OAuthSettings.TeamMemberships.OAuthClaimMappings.some((m) => !(m.ClaimValRegex && m.Team));
      if (hasInvalidMapping) {
        return false;
      }
    }
    return true;
  }

  function initView() {
    $q.all({
      settings: SettingsService.settings(),
      teams: TeamService.teams(),
    })
      .then(function success(data) {
        var settings = data.settings;
        $scope.teams = data.teams;
        $scope.settings = settings;

        $scope.OAuthSettings = settings.OAuthSettings;
        $scope.authMethod = settings.AuthenticationMethod;
        if (settings.AuthenticationMethod === AuthenticationMethod.LDAP && settings.LDAPSettings.ServerType === SERVER_TYPES.AD) {
          $scope.authMethod = AuthenticationMethod.AD;
        }

        if (settings.LDAPSettings.URL) {
          settings.LDAPSettings.URLs = [settings.LDAPSettings.URL];
        }
        if (!settings.LDAPSettings.URLs) {
          settings.LDAPSettings.URLs = [];
        }
        if (!settings.LDAPSettings.URLs.length) {
          settings.LDAPSettings.URLs.push('');
        }
        if (!settings.LDAPSettings.ServerType) {
          settings.LDAPSettings.ServerType = SERVER_TYPES.CUSTOM;
        }

        $scope.formValues.ldap.serverType = settings.LDAPSettings.ServerType;
        if (settings.LDAPSettings.ServerType === SERVER_TYPES.AD) {
          $scope.formValues.ldap.adSettings = settings.LDAPSettings;
        } else {
          $scope.formValues.ldap.ldapSettings = Object.assign($scope.formValues.ldap.ldapSettings, settings.LDAPSettings);
        }
        $scope.state.isEditLDAP = settings.LDAPSettings.ServerType === SERVER_TYPES.AD || settings.LDAPSettings.ServerType === SERVER_TYPES.LDAP;
        $scope.state.initialServerType = settings.LDAPSettings.ServerType;
      })
      .catch(function error(err) {
        Notifications.error('Failure', err, 'Unable to retrieve application settings');
      });
  }

  initView();
}