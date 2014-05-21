# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import shutil
import simplejson as json
import symbolicate
import unittest

# python -m unittest telemetry.test_convert
#   - or -
# coverage run -m telemetry.test_convert; coverage html

class SymbolicateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_signature(self):
        test_stack = ["0x3c09c (in wntdll.pdb)",
        "0x153be (in wkernelbase.pdb)",
        "0x1537f (in wkernelbase.pdb)",
        "0x156a6 (in wkernelbase.pdb)",
        "Interesting thing (in version.pdb)",
        "0xc2e708 (in xul.pdb)",
        "Another interesting thing (in version.pdb)",
        "nsAppShell::Run() (in xul.pdb)", # <-- boring
        "0x4c4502 (in xul.pdb)"]
        symbolicated = symbolicate.get_signature(test_stack)
        self.assertEqual(test_stack[4:7], symbolicated)
        self.assertEqual(len(symbolicated), 3)

    def test_long_sig(self):
        test_stack = [
            "RealMsgWaitForMultipleObjectsEx (in user32.pdb)",
            "MsgWaitForMultipleObjects (in user32.pdb)",
            "mozilla::ipc::MessageChannel::WaitForInterruptNotify() (in xul.pdb)",
            "mozilla::ipc::MessageChannel::InterruptCall(IPC::Message *,IPC::Message *) (in xul.pdb)",
            "mozilla::ipc::MessageChannel::Call(IPC::Message *,IPC::Message *) (in xul.pdb)",
            "mozilla::plugins::PPluginInstanceParent::CallNPP_SetWindow(mozilla::plugins::NPRemoteWindow const &) (in xul.pdb)",
            "mozilla::plugins::PluginInstanceParent::NPP_SetWindow(_NPWindow const *) (in xul.pdb)",
            "mozilla::plugins::PluginModuleParent::NPP_SetWindow(_NPP *,_NPWindow *) (in xul.pdb)",
            "nsNPAPIPluginInstance::SetWindow(_NPWindow *) (in xul.pdb)",
            "nsPluginNativeWindow::CallSetWindow(nsRefPtr<nsNPAPIPluginInstance> &) (in xul.pdb)",
            "nsPluginNativeWindowWin::CallSetWindow(nsRefPtr<nsNPAPIPluginInstance> &) (in xul.pdb)",
            "nsObjectFrame::CallSetWindow(bool) (in xul.pdb)",
            "nsPluginInstanceOwner::CallSetWindow() (in xul.pdb)",
            "nsPluginInstanceOwner::UpdateWindowPositionAndClipRect(bool) (in xul.pdb)",
            "nsObjectFrame::SetIsDocumentActive(bool) (in xul.pdb)",
            "SetPluginIsActive (in xul.pdb)",
            "EnumerateFreezables (in xul.pdb)",
            "nsTHashtable<nsBaseHashtableET<nsStringHashKey,`anonymous namespace'::TelemetryIOInterposeObserver::FileStatsByStage> >::s_EnumStub(PLDHashTable *,PLDHashEntryHdr *,unsigned int,void *) (in xul.pdb)",
            "PL_DHashTableEnumerate(PLDHashTable *,PLDHashOperator (*)(PLDHashTable *,PLDHashEntryHdr *,unsigned int,void *),void *) (in xul.pdb)",
            "nsTHashtable<nsPtrHashKey<nsIContent> >::EnumerateEntries(PLDHashOperator (*)(nsPtrHashKey<nsIContent> *,void *),void *) (in xul.pdb)",
            "PresShell::SetIsActive(bool) (in xul.pdb)",
            "nsDocShell::SetIsActive(bool) (in xul.pdb)",
            "NS_InvokeByIndex (in xul.pdb)",
            "XPC_WN_GetterSetter(JSContext *,unsigned int,JS::Value *) (in xul.pdb)",
            "js::Invoke(JSContext *,JS::CallArgs,js::MaybeConstruct) (in mozjs.pdb)",
            "js::Invoke(JSContext *,JS::Value const &,JS::Value const &,unsigned int,JS::Value const *,JS::MutableHandle<JS::Value>) (in mozjs.pdb)",
            "js::Shape::set(JSContext *,JS::Handle<JSObject *>,JS::Handle<JSObject *>,bool,JS::MutableHandle<JS::Value>) (in mozjs.pdb)",
            "js::NativeSet<0>(JSContext *,JS::Handle<JSObject *>,JS::Handle<JSObject *>,JS::Handle<js::Shape *>,bool,JS::MutableHandle<JS::Value>) (in mozjs.pdb)",
            "js::baseops::SetPropertyHelper<0>(JSContext *,JS::Handle<JSObject *>,JS::Handle<JSObject *>,JS::Handle<jsid>,js::baseops::QualifiedBool,JS::MutableHandle<JS::Value>,bool) (in mozjs.pdb)"
        ]
        symbolicated = symbolicate.get_signature(test_stack)
        self.assertEqual(len(symbolicated), 15)
        self.assertEqual(test_stack[1:16], symbolicated)

    def test_interesting(self):
        t = "RealMsgWaitForMultipleObjectsEx (in wuser32.pdb)"
        self.assertTrue(symbolicate.is_irrelevant(t))

    def test_irrelevance(self):
        # RealMsgWaitForMultipleObjectsEx (in wuser32.pdb) should be filtered as an irrelevant frame by
        # '(Nt|Zw)?WaitForMultipleObjects(Ex)?'
        test_stack = [
            "RealMsgWaitForMultipleObjectsEx (in wuser32.pdb)",
            "MsgWaitForMultipleObjects (in wuser32.pdb)",
            "mozilla::ipc::MessageChannel::WaitForInterruptNotify() (in xul.pdb)",
            "mozilla::ipc::MessageChannel::InterruptCall(IPC::Message *,IPC::Message *) (in xul.pdb)",
            "mozilla::ipc::MessageChannel::Call(IPC::Message *,IPC::Message *) (in xul.pdb)",
            "mozilla::plugins::PPluginInstanceParent::CallNPP_Destroy(short *) (in xul.pdb)",
            "mozilla::plugins::PluginInstanceParent::Destroy() (in xul.pdb)",
            "mozilla::plugins::PluginModuleParent::NPP_Destroy(_NPP *,_NPSavedData * *) (in xul.pdb)",
            "nsNPAPIPluginInstance::Stop() (in xul.pdb)",
            "nsPluginHost::StopPluginInstance(nsNPAPIPluginInstance *) (in xul.pdb)",
            "nsObjectLoadingContent::DoStopPlugin(nsPluginInstanceOwner *,bool,bool) (in xul.pdb)",
            "nsObjectLoadingContent::StopPluginInstance() (in xul.pdb)",
            "nsObjectLoadingContent::UnloadObject(bool) (in xul.pdb)",
            "CheckPluginStopEvent::Run() (in xul.pdb)",
            "nsBaseAppShell::RunSyncSectionsInternal(bool,unsigned int) (in xul.pdb)"
        ]
        symbolicated = symbolicate.get_signature(test_stack)
        self.assertEqual(len(symbolicated), 13)
        self.assertEqual(test_stack[1:14], symbolicated)

if __name__ == "__main__":
    unittest.main()
