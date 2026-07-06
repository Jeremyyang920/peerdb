'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import {
  blankSSHConfig,
  sshSetting,
} from '@/app/peers/create/[peerType]/helpers/ssh';
import InfoPopover from '@/components/InfoPopover';
import {
  handleFieldChange,
  handleSSHParam,
} from '@/components/PeerForms/common';
import { CockroachConfig, SSHConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useEffect, useState } from 'react';

interface CockroachDBProps {
  settings: PeerSetting[];
  setter: PeerSetter;
  config: CockroachConfig;
}

export default function CockroachDBForm({
  settings,
  setter,
  config,
}: CockroachDBProps) {
  const [showSSH, setShowSSH] = useState(false);
  const [sshConfig, setSSHConfig] = useState(blankSSHConfig);

  useEffect(() => {
    setter((prev) => ({
      ...prev,
      sshConfig: showSSH ? sshConfig : undefined,
    }));
  }, [sshConfig, setter, showSSH]);

  return (
    <>
      {settings.map((setting, id) => {
        if (setting.type === 'switch') {
          return (
            <RowWithSwitch
              key={id}
              label={
                <Label>
                  {setting.label}{' '}
                  {!setting.optional && (
                    <Tooltip
                      style={{ width: '100%' }}
                      content='This is a required field.'
                    >
                      <Label colorName='lowContrast' colorSet='destructive'>
                        *
                      </Label>
                    </Tooltip>
                  )}
                </Label>
              }
              action={
                <div>
                  <Switch
                    onCheckedChange={(val: boolean) =>
                      setting.stateHandler(val, setter)
                    }
                  />
                  {setting.tips && (
                    <InfoPopover
                      tips={setting.tips}
                      link={setting.helpfulLink}
                    />
                  )}
                </div>
              }
            />
          );
        }

        return (
          <div key={id}>
            <RowWithTextField
              label={
                <Label>
                  {setting.label}{' '}
                  {!setting.optional && (
                    <Tooltip content='This is a required field.'>
                      <Label colorName='lowContrast' colorSet='destructive'>
                        *
                      </Label>
                    </Tooltip>
                  )}
                </Label>
              }
              action={
                <div
                  style={{
                    display: 'flex',
                    flexDirection: 'row',
                    alignItems: 'center',
                  }}
                >
                  <TextField
                    variant='simple'
                    style={
                      setting.type === 'file'
                        ? { border: 'none', height: 'auto' }
                        : { border: 'auto' }
                    }
                    type={setting.type}
                    defaultValue={
                      setting.field && config
                        ? ((config as any)[setting.field] as string)
                        : (setting.default as string)
                    }
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      handleFieldChange(e, setting, setter)
                    }
                  />
                  {setting.tips && (
                    <InfoPopover
                      tips={setting.tips}
                      link={setting.helpfulLink}
                    />
                  )}
                </div>
              }
            />
          </div>
        );
      })}

      <Label
        as='label'
        style={{ marginTop: '1rem', display: 'block' }}
        variant='subheadline'
        colorName='lowContrast'
      >
        SSH Configuration
      </Label>
      <Label>
        You may provide SSH configuration to connect to your CockroachDB cluster
        through an SSH tunnel.
      </Label>
      <div style={{ width: '50%', display: 'flex', alignItems: 'center' }}>
        <Label variant='subheadline'>Configure SSH Tunnel</Label>
        <Switch onCheckedChange={(state) => setShowSSH(state)} />
      </div>
      {showSSH &&
        sshSetting.map((sshParam, index) => (
          <RowWithTextField
            key={index}
            label={
              <Label>
                {sshParam.label}{' '}
                {!sshParam.optional && (
                  <Tooltip
                    style={{ width: '100%' }}
                    content='This is a required field.'
                  >
                    <Label colorName='lowContrast' colorSet='destructive'>
                      *
                    </Label>
                  </Tooltip>
                )}
              </Label>
            }
            action={
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'row',
                  alignItems: 'center',
                }}
              >
                <TextField
                  variant='simple'
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    handleSSHParam(e, sshParam, setSSHConfig)
                  }
                  style={{
                    border: sshParam.type === 'file' ? 'none' : 'auto',
                    height: sshParam.type === 'textarea' ? '15rem' : 'auto',
                  }}
                  type={sshParam.type}
                  defaultValue={
                    (sshConfig as SSHConfig)[
                      sshParam.label === 'SSH Private Key'
                        ? 'privateKey'
                        : sshParam.label === "Host's Public Key"
                          ? 'hostKey'
                          : (sshParam.label.toLowerCase() as keyof SSHConfig)
                    ] || ''
                  }
                />
                {sshParam.tips && <InfoPopover tips={sshParam.tips} />}
              </div>
            }
          />
        ))}
    </>
  );
}
