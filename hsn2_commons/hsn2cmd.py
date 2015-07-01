# Copyright (c) NASK, NCSC
#
# This file is part of HoneySpider Network 2.0.
#
# This is a free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import datetime
import sys

from hsn2_protobuf import Config_pb2
from hsn2_protobuf import Info_pb2
from hsn2_protobuf import Jobs_pb2
from hsn2_protobuf import Service_pb2
from hsn2_protobuf import Workflows_pb2


class CommandDispatcherMessage(Exception):
    pass


class CommandDispatcher(object):
    '''
    Responsible for sending commands to the Framework. Used by the HSN 2 Console.
    '''

    def __init__(self, bus, logger, cliargs, config=None, verbose=True, timeout=None):
        '''
        @param bus: initialized HSN2 bus object.
        @param logger: A Logger object.
        @param cliargs: A parsed command line parameters object.
        @param config: A ConfigParser object containing the loaded configuration file.
        @param verbose: Modifies the verbosity of the dispatcher. Controls whether messages are printed to stdout or handled differently.
        '''
        self.bus = bus
        self.logger = logger
        self.cliargs = cliargs
        self.verbose = verbose

        if timeout is None:
            self.timeout = config.getint("core", "timeout")
        else:
            self.timeout = timeout
        self.logger.debug("Timeout set on %ds", self.timeout)

    def dispatch(self, command, aliases):
        '''
        Responsible for selecting the class method which is to be invoked depending on the invoked command and calling it.
        @param command: The command that was invoked.
        @param aliases: The aliases module.
        '''
        self.logger.debug("Executing command: %s", command)
        command = aliases.getFullName(command)
        fun = self.commandsList.get(command, None)
        if (isinstance(fun, dict)):
            subcommand = self.cliargs.__dict__.get(command)
            subcommand = aliases.getFullName(subcommand, command)
            fun = fun.get(subcommand)
        if (fun is not None):
            return fun(self)
        else:
            msg = "ERROR: incorrect command '%s'" % command
            if self.verbose:
                print msg
            else:
                raise CommandDispatcherMessage(msg)

    def _command_job_submit(self):
        '''
        Submits a job for processing.
        '''
        if self.verbose:
            print("The job descriptor (workflow=%s) is outgoing..." % self.cliargs.jd_workflow)
        # setup service parameters
        servicesParams = {}
        if (self.cliargs.param is not False):
            import re
            expr = re.compile("([^=]+)\.([^=]+)=(.+)")
            for par in self.cliargs.param:
                for p in par:
                    m = expr.match(p)
                    if (m is None or len(m.groups()) != 3 or len(m.group(1)) == 0 or len(m.group(2)) == 0 or len(m.group(3)) == 0):
                        if self.verbose:
                            print "ERROR: incorrect parameter '%s'" % p
                            print "Expected pattern is 'service_label.param_name=param_value'"
                        else:
                            raise CommandDispatcherMessage(
                                "incorrect parameter '%s'. Expected pattern is 'service_label.param_name=param_value'" % p)
                        sys.exit()
                    label = m.group(1)
                    par = Service_pb2.Parameter()
                    par.name = m.group(2)
                    par.value = unicode(m.group(3).decode('utf-8'))
                    if (label not in servicesParams):
                        servicesParams[label] = []
                    servicesParams[label].append(par)
        # start build JobDescriptor
        jd = Jobs_pb2.JobDescriptor()
        jd.workflow = self.cliargs.jd_workflow

        # assign parameters
        for s in servicesParams.keys():
            sc = jd.config.add()
            sc.service_label = s
            for p in servicesParams[s]:
                tp = sc.parameters.add()
                tp.name = p.name
                tp.value = p.value
        (mtype, resp) = self.bus.sendCommand("fw", "JobDescriptor", jd,
                                             1, self.timeout)

        if mtype == "JobAccepted":
            ja = Jobs_pb2.JobAccepted()
            ja.ParseFromString(resp)
            if self.verbose:
                print "The job has been accepted. (Id=%s)" % ja.job
            else:
                return ja.job
        elif mtype == "JobRejected":
            jr = Jobs_pb2.JobRejected()
            jr.ParseFromString(resp)
            msg = "The job has been rejected. The reason is '%s'." % jr.reason
            if self.verbose:
                print msg
            else:
                raise CommandDispatcherMessage(msg)
        else:
            msg = "Unexpected response received (type=%s)." % mtype
            if self.verbose:
                print msg
            else:
                raise CommandDispatcherMessage(msg)

    def _command_job_list(self):
        '''
        Retrieves a list of jobs.
        '''
        request = Jobs_pb2.JobListRequest()
        (mtype, resp) = self.bus.sendCommand("fw", "JobListRequest", request,
                                             1, self.timeout)
        if mtype == "JobListReply":
            jr = Jobs_pb2.JobListReply()
            jr.ParseFromString(resp)
            if self.verbose:
                if jr.jobs.__len__() == 0:
                    print "There are no jobs in the framework."
                else:
                    print "Found %d job(s):" % jr.jobs.__len__()
                    for job in jr.jobs:
                        print "   id: %s status: %s" \
                            % (job.id, self._getJobStatusString(job.status))
            else:
                jobs_dict = {}
                for job in jr.jobs:
                    jobs_dict[job.id] = job.status
                return jobs_dict
        else:
            msg = "Unexpected response received (type=%s)." % mtype
            if self.verbose:
                print msg
            else:
                raise CommandDispatcherMessage(msg)

    def _command_config_get(self):
        '''
        Retrieves the framework configuration.
        '''
        if self.verbose:
            print "Config get request called"
        request = Config_pb2.GetConfigRequest()
        (mtype, resp) = self.bus.sendCommand(
            "fw", "GetConfigRequest", request,
            1, self.timeout)
        if mtype == "GetConfigReply":
            cr = Config_pb2.GetConfigReply()
            cr.ParseFromString(resp)
            if self.verbose:
                for prop in cr.properties:
                    print "   %s\t%s" % (prop.name, prop.value)
            else:
                properties = []
                for prop in cr.properties:
                    properties.append(prop)
                return properties
        else:
            msg = "Unexpected response received (type=%s)." % mtype
            if self.verbose:
                print msg
            else:
                raise CommandDispatcherMessage(msg)

    def _command_config_set(self):
        '''
        Sends a new framework configuration to the framework.
        TODO: implement (task #4078). Blocked by lack of support for command in framework (#4942).
        '''
        print "Config set request called"
        # setup config parameters
        configProps = []
        if (self.cliargs.param is not None):
            import re
            expr = re.compile("(.*)=(.*)")
            for par in self.cliargs.param:
                for p in par:
                    m = expr.match(p)
                    if (m is None or len(m.groups()) != 2 or len(m.group(1)) == 0 or len(m.group(2)) == 0):
                        print "ERROR: incorrect parameter '%s'" % p
                        print "Expected pattern is 'service_label.param_name=param_value'"
                        sys.exit()
                    par = Config_pb2.Property()
                    par.name = m.group(1)
                    par.value = m.group(2)
                    configProps.append(par)

        request = Config_pb2.SetConfigRequest()
        request.replace = self.cliargs.replace
        # assign parameters
        for p in configProps:
            prop = request.properties.add()
            prop.name = p.name
            prop.value = p.value

        (mtype, resp) = self.bus.sendCommand(
            "fw", "SetConfigRequest", request,
            1, self.timeout)
        if mtype == "SetConfigReply":
            cr = Config_pb2.SetConfigReply()
            if (cr.success == True):
                print "Set config was successful"
            else:
                print "Set config failed"
            cr.ParseFromString(resp)
            for prop in cr.properties:
                print "   %s\t%s" % (prop.name, prop.value)
        else:
            print "Unexpected response received (type=%s)." % mtype

    def _command_job_details(self):
        '''
        Retrieves details about a specific job.
        '''
        if self.verbose:
            print "Job details called"
        request = Info_pb2.InfoRequest()
        request.type = Info_pb2.JOB
        request.id = int(self.cliargs.gjd_id)
        (mtype, resp) = self.bus.sendCommand("fw", "InfoRequest", request,
                                             1, self.timeout)
        if mtype == "InfoError":
            ie = Info_pb2.InfoError()
            ie.ParseFromString(resp)
            msg = "There is a problem with the request. The reason is '%s'." % ie.reason
            if self.verbose:
                print msg
            else:
                raise CommandDispatcherMessage(msg)
        elif mtype == "InfoData":
            if self.verbose:
                print "Details about the job:"
            infoData = Info_pb2.InfoData()
            infoData.ParseFromString(resp)
            attributes = {}
            datetimeAttributes = {}
            for att in infoData.data.attrs:
                if (att.type == 2):
                    attributes[att.name] = att.data_int
                elif (att.type == 3):
                    ts = att.data_time / 1000
                    attributes[att.name] = datetime.datetime.fromtimestamp(
                        ts).strftime("%Y-%m-%d %H:%M:%S")
                    datetimeAttributes[
                        att.name] = datetime.datetime.fromtimestamp(ts)
                elif (att.type == 5):
                    attributes[att.name] = att.data_string
            if self.verbose:
                for att in attributes.keys():
                    print "   %s\t%s" % (att, attributes[att])
            else:
                for att in datetimeAttributes.keys():
                    attributes[att] = datetimeAttributes[att]
                return attributes
        else:
            msg = "Unexpected response received (type=%s)." % mtype
            if self.verbose:
                print msg
            else:
                raise CommandDispatcherMessage(msg)

    def _command_ping(self):
        '''
        Checks if the framework is alive by sending a Ping and waiting for a pong.
        '''
        if self.verbose:
            print "The ping is outgoing..."
        (mtype, resp) = self.bus.sendCommand("fw", "Ping", "",
                                             1, self.timeout)
        if (mtype == "Ping" and resp == "pong"):
            if self.verbose:
                print "The framework is alive."
            else:
                return True
        else:
            if self.verbose:
                print "Unknown response!"
            else:
                return False

    def _command_workflow_list(self):
        '''
        Retrieves a list of workflows.
        '''
        if self.verbose:
            print "The list workflows is outgoing..."
        lw = Workflows_pb2.WorkflowListRequest()
        lw.enabled_only = self.cliargs.enabled
        (mtype, resp) = self.bus.sendCommand("fw", "WorkflowListRequest",
                                             lw, 1, self.timeout)
        if mtype == "WorkflowListReply":
            lwr = Workflows_pb2.WorkflowListReply()
            lwr.ParseFromString(resp)
            if self.verbose:
                print "Found %d workflow(s):" % lwr.workflows.__len__()
                for wf in lwr.workflows:
                    print "	  name:	%s" % wf.name
                    print "	  enabled: %s" % wf.enabled
                    print ""
            else:
                workflows = []
                for wf in lwr.workflows:
                    workflows.append(wf)
                return workflows

        elif mtype == "WorkflowError":
            werr = Workflows_pb2.WorkflowError()
            werr.ParseFromString(resp)
            print "Unsuccessful operation because: %s" % werr.reason

        else:
            msg = "Unexpected response received (type=%s)." % mtype
            if self.verbose:
                print msg
            else:
                raise CommandDispatcherMessage(msg)

    def _command_workflow_upload(self):
        '''
        Uploads a new workflow to the framework.
        '''
        aw = Workflows_pb2.WorkflowUploadRequest()
        aw.name = self.cliargs.name
        if self.cliargs.file:
            aw.content = unicode(self.cliargs.file.read().decode('utf-8'))
        else:
            aw.content = ''.join(sys.stdin.readlines())
        aw.overwrite = self.cliargs.overwrite
        print "Uploading workflow to framework."
        (mtype, resp) = self.bus.sendCommand("fw", "WorkflowUploadRequest",
                                             aw, 1, self.timeout)
        if mtype == "WorkflowUploadReply":
            awr = Workflows_pb2.WorkflowUploadReply()
            awr.ParseFromString(resp)
            print "Workflow uploaded as revision %s" % awr.revision
        else:
            print "Unexpected response received (type=%s)." % mtype

    def _command_workflow_get(self):
        '''
        Retrieves a workflow.
        '''
        gw = Workflows_pb2.WorkflowGetRequest()
        gw.name = self.cliargs.jd_workflow
        if self.cliargs.revision is not None:
            gw.revision = self.cliargs.revision
        self.logger.info("Get workflow is being sent")
        (mtype, resp) = self.bus.sendCommand("fw", "WorkflowGetRequest",
                                             gw, 1, self.timeout)
        if mtype == "WorkflowGetReply":
            gwr = Workflows_pb2.WorkflowGetReply()
            gwr.ParseFromString(resp)
            try:
                if self.cliargs.file:
                    self.cliargs.file.write(gwr.content.encode('utf-8'))
                elif self.verbose:
                    sys.stdout.write(gwr.content)
                else:
                    return gwr.content
            except IOError as e:
                self.logger.warn(
                    "Couldn't open destination file '%s'. Printing to stdout:" % e.message)
                sys.stdout.write(gwr.content)
        else:
            msg = "Unexpected response received (type=%s)." % mtype
            if self.verbose:
                print "Unexpected response received (type=%s)." % mtype
            else:
                raise CommandDispatcherMessage(msg)

    def _command_workflow_enable(self):
        '''
        Enable a specific workflow.
        '''
        self._command_workflow_policy(True)

    def _command_workflow_disable(self):
        '''
        Disable a specific workflow.
        '''
        self._command_workflow_policy(False)

    def _command_workflow_policy(self, enabled):
        '''
        Enables/disables a specific workflow.
        @param enabled: If True then the workflow will be enabled, otherwise it will be disabled.
        '''
        pw = Workflows_pb2.WorkflowPolicyRequest()
        pw.name = self.cliargs.jd_workflow
        pw.enabled = enabled
        print "Workflow policy change is being sent"
        (mtype, resp) = self.bus.sendCommand("fw", "WorkflowPolicyRequest",
                                             pw, 1, self.timeout)
        if mtype == "WorkflowPolicyReply":
            pwr = Workflows_pb2.WorkflowPolicyReply()
            pwr.ParseFromString(resp)
            print "Previous policy was: %s:" % str(pwr.previous)

        elif mtype == "WorkflowError":
            werr = Workflows_pb2.WorkflowError()
            werr.ParseFromString(resp)
            print "Unsuccessful operation because: %s" % werr.reason

        else:
            print "Unexpected response received (type=%s)." % mtype

    def _command_workflow_history(self):
        '''
        Retrieve a workflow's modification history.
        '''
        hw = Workflows_pb2.WorkflowHistoryRequest()
        hw.name = self.cliargs.jd_workflow

        if self.verbose:
            print "Workflow history request is being sent"
        (mtype, resp) = self.bus.sendCommand("fw", "WorkflowHistoryRequest",
                                             hw, 1, self.timeout)
        if mtype == "WorkflowHistoryReply":
            hwr = Workflows_pb2.WorkflowHistoryReply()
            hwr.ParseFromString(resp)
            if self.verbose:
                head = "Found %s revisions of '%s':" % (
                    len(hwr.history), hw.name)
                print
                print head
                print "-" * len(head)
                for item in hwr.history:
                    timestring = datetime.datetime.fromtimestamp(item.mtime)
                    timestring = timestring.strftime("%Y-%m-%d %H:%M")
                    print "%s %s" % (item.revision, timestring)
            else:
                return hwr.history
        elif mtype == "WorkflowError":
            werr = Workflows_pb2.WorkflowError()
            werr.ParseFromString(resp)
            if self.verbose:
                print "Unsuccessful operation because: %s" % werr.reason
            else:
                raise CommandDispatcherMessage(werr)
        else:
            if self.verbose:
                print "Unexpected response received (type=%s)." % mtype
            else:
                raise CommandDispatcherMessage(mtype)

    def _command_workflow_status(self):
        '''
        Retrieve the workflow's status.
        '''
        sw = Workflows_pb2.WorkflowStatusRequest()
        sw.name = self.cliargs.jd_workflow
        if self.cliargs.revision is not None:
            sw.revision = self.cliargs.revision
        if self.verbose:
            print "Workflow status request is being sent"
        (mtype, resp) = self.bus.sendCommand("fw", "WorkflowStatusRequest",
                                             sw, 1, self.timeout)
        if mtype == "WorkflowStatusReply":
            swr = Workflows_pb2.WorkflowStatusReply()
            swr.ParseFromString(resp)
            if swr.valid:
                valid = "valid"
            else:
                valid = "invalid"
            if swr.enabled:
                enabled = "enabled"
            else:
                enabled = "disabled"
            if self.verbose:
                print "%s %s %s %s" % (swr.info.revision, valid, enabled, swr.description)
            else:
                return swr
        elif mtype == "WorkflowError":
            werr = Workflows_pb2.WorkflowError()
            werr.ParseFromString(resp)
            msg = "Unsuccessful operation because: %s" % werr.reason
            if self.verbose:
                print msg
            else:
                raise CommandDispatcherMessage(msg)
        else:
            msg = "Unexpected response received (type=%s)." % mtype
            if self.verbose:
                print "Unexpected response received (type=%s)." % mtype
            else:
                raise CommandDispatcherMessage(msg)

    def _getJobStatusString(self, status):
        '''
        Retrives the name of the status by it's id.
        @param status: The id of the status.
        @return: name of the status
        '''
        return Jobs_pb2._JOBSTATUS.values_by_number[status].name

    jobCommands = {
        'list': _command_job_list,
        'details': _command_job_details,
        'submit': _command_job_submit
    }

    workflowCommands = {
        'list': _command_workflow_list,
        'get': _command_workflow_get,
        'upload': _command_workflow_upload,
        'disable': _command_workflow_disable,
        'enable': _command_workflow_enable,
        'history': _command_workflow_history,
        'status': _command_workflow_status
    }

    configCommands = {
        'get': _command_config_get,
        'set': _command_config_set
    }

    commandsList = {
        'ping': _command_ping,
        'job': jobCommands,
        'workflow': workflowCommands,
        'config': configCommands
    }
