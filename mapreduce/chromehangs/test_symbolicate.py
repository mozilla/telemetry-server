# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime
import os
import shutil
import simplejson as json
import symbolicate
import unittest

# python -m unittest test_symbolicate
#   - or -
# coverage run -m test_symbolicate; coverage html

class SymbolicateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_delta_sec(self):
        d1 = datetime(2014, 5, 22, 13, 17, 41, 0)
        d2 = datetime(2014, 5, 22, 13, 17, 51, 0)
        self.assertEqual(10, symbolicate.delta_sec(d1, d2))

        dnow = symbolicate.delta_sec(d1)
        self.assertTrue(dnow > 10)

    def test_new_request(self):
        r = symbolicate.new_request()
        self.assertEqual([], r["stacks"])
        self.assertEqual([], r["memoryMap"])
        self.assertEqual(3, r["version"])

    def test_is_interesting(self):
        # interesting means:
        #  not irrelevant
        #  not raw
        #  not js
        tests = [
            'RealMsgWaitForMultipleObjectsEx (in wuser32.pdb)', # irrelevant
            "0x156a6 (in wkernelbase.pdb)", # raw
            "js::RunScript(JSContext *,js::RunState &) (in mozjs.pdb)", # js
            "Interesting thing (in version.pdb)",
            "0xc2e708 (in xul.pdb)",  # raw
            "Another interesting thing (in version.pdb)",
            "nsAppShell::Run() (in xul.pdb)", # boring (and yet interesting)
            "0x4c4502 (in xul.pdb)"
        ]
        expecteds = [
            False,
            False,
            False,
            True,
            False,
            True,
            True,
            False
        ]
        for i in range(len(tests)):
            actual = symbolicate.is_interesting(tests[i])
            msg = "Expected {} for {}".format(expecteds[i], tests[i])
            self.assertEqual(expecteds[i], actual, msg=msg)

    def test_is_irrelevant(self):
        tests     = [
            'RealMsgWaitForMultipleObjectsEx (in wuser32.pdb)',
            '@0xdeadbeef (in cow.pdb)',
            'Interesting thing (in version.pdb)',
            'MessageLoop::RunHandler() (in xul.pdb)'
        ]
        expecteds = [
            True,
            True,
            False,
            False
        ]
        for i in range(len(tests)):
            self.assertEqual(expecteds[i], symbolicate.is_irrelevant(tests[i]))

    def test_is_raw(self):
        tests     = [
            'RealMsgWaitForMultipleObjectsEx (in wuser32.pdb)',
            '0xdeadbeef (in cow.pdb)',
            'Interesting thing (in version.pdb)',
            'MessageLoop::RunHandler() (in xul.pdb)',
            '',
            'zero',
            "0x156a6 (in wkernelbase.pdb)",
            "-0x156a6 (in wkernelbase.pdb)",
        ]
        expecteds = [
            False,
            True,
            False,
            False,
            False,
            False,
            True,
            True
        ]
        for i in range(len(tests)):
            actual = symbolicate.is_raw(tests[i])
            msg = "Expected {} for {}".format(expecteds[i], tests[i])
            self.assertEqual(expecteds[i], actual, msg=msg)

    def test_is_js(self):
        tests = [
            'js::RealMsgWaitForMultipleObjectsEx (in wuser32.pdb)',
            'js::hello there!',
            'js::jit::DoGetNameFallback (in mozjs.pdb)',
            'Interesting thing (in version.pdb)',
            'MessageLoop::RunHandler() (in xul.pdb)',
            '',
            'zero'
        ]
        expecteds = [
            True,
            True,
            True,
            False,
            False,
            False,
            False
        ]
        for i in range(len(tests)):
            actual = symbolicate.is_js(tests[i])
            msg = "Expected {} for {}".format(expecteds[i], tests[i])
            self.assertEqual(expecteds[i], actual, msg=msg)

    def test_is_boring(self):
        tests     = [
            "NS_ProcessNextEvent_P(nsIThread *,bool) (in xul.pdb)",
            "mozilla::ipc::MessagePump::Run(base::MessagePump::Delegate *) (in xul.pdb)",
            "MessageLoop::RunHandler() (in xul.pdb)",
            "MessageLoop::Run() (in xul.pdb)",
            "nsBaseAppShell::Run() (in xul.pdb)",
            "nsAppShell::Run() (in xul.pdb)",
            "nsAppStartup::Run() (in xul.pdb)",
            "XREMain::XRE_mainRun() (in xul.pdb)",
            "XREMain::XRE_main(int,char * * const,nsXREAppData const *) (in xul.pdb)",
            "0x156a6 (in wkernelbase.pdb)",
            "Interesting thing (in version.pdb)",
            "RealMsgWaitForMultipleObjectsEx (in user32.pdb)",
            ''
        ]
        expecteds = [
            True,
            True,
            True,
            True,
            True,
            True,
            True,
            True,
            True,
            False,
            False,
            False,
            False,
            False
        ]
        for i in range(len(tests)):
            actual = symbolicate.is_boring(tests[i])
            msg = "Expected {} for {}".format(expecteds[i], tests[i])
            self.assertEqual(expecteds[i], actual, msg=msg)

    def test_is_sentinel(self):
        tests     = [
            "mozilla::ipc::MessageChannel::Call(IPC::Message *,IPC::Message *) (in xul.pdb)",
            '',
            "nsDocShell::SetIsActive(bool) (in xul.pdb)",
            "0x156a6 (in wkernelbase.pdb)"
        ]
        expecteds = [
            True,
            False,
            False,
            False
        ]
        for i in range(len(tests)):
            actual = symbolicate.is_sentinel(tests[i])
            msg = "Expected {} for {}".format(expecteds[i], tests[i])
            self.assertEqual(expecteds[i], actual, msg=msg)

    def test_is_interesting_lib(self):
        tests     = [
            "Test::Function() (in xul.pdb)",
            "Test::Function() (in firefox.pdb)",
            "Test::Function() (in mozjs.pdb)",
            "Test::Function() (in boring.pdb)",
            "Test::Function() (in exciting.pdb)",
            ''
        ]
        expecteds = [
            True,
            True,
            True,
            False,
            False,
            False
        ]
        for i in range(len(tests)):
            actual = symbolicate.is_interesting_lib(tests[i])
            msg = "Expected {} for {}".format(expecteds[i], tests[i])
            self.assertEqual(expecteds[i], actual, msg=msg)

    def test_clean_element(self):
        tests = [
            "0x3c09c (in wntdll.pdb)",
            "0x153be (in wkernelbase.pdb)",
            "0x1537f (in xul.pdb)",
            "Interesting thing (in version.pdb)",
            "nsAppShell::Run() (in xul.pdb)", # <-- boring
            "",
            "js::Invoke(JSContext *,JS::CallArgs,js::MaybeConstruct) (in mozjs.pdb)",
            "0x153be",
            "0x153be with some extra jazz",
            "js::Invoke(JSContext *,JS::CallArgs,js::MaybeConstruct)"
        ]
        expecteds = [
            "-0x1 (in wntdll.pdb)",
            "-0x1 (in wkernelbase.pdb)",
            "-0x1 (in xul.pdb)",
            "Interesting thing (in version.pdb)",
            "nsAppShell::Run() (in xul.pdb)", # <-- boring
            "",
            "JS Frame (in mozjs.pdb)",
            "-0x1",
            "-0x1 with some extra jazz",
            "JS Frame"
        ]

        for i in range(len(tests)):
            actual = symbolicate.clean_element(tests[i])
            self.assertEqual(expecteds[i], actual)

    def test_min_json(self):
        o = {"a": 1, "b": 2}
        self.assertEqual('{"a":1,"b":2}', symbolicate.min_json(o))

    def get_test_stacks(self):
        return {
            # name: { stack: [], expected: [] }
            "normal signature": {
                "stack": [
                    "0x3c09c (in wntdll.pdb)",
                    "0x153be (in wkernelbase.pdb)",
                    "0x1537f (in wkernelbase.pdb)",
                    "0x156a6 (in wkernelbase.pdb)",
                    "Interesting thing (in version.pdb)",
                    "0xc2e708 (in xul.pdb)",
                    "Another interesting thing (in version.pdb)",
                    "nsAppShell::Run() (in xul.pdb)", # <-- boring
                    "0x4c4502 (in xul.pdb)"
                ],
                "expected": [
                    "Interesting thing (in version.pdb)",
                    "-0x1 (in xul.pdb)",
                    "Another interesting thing (in version.pdb)"
                ]
            },
            "completely boring": {
                "stack": [
                    "0x3c09c (in wntdll.pdb)",
                    "0x153be (in wkernelbase.pdb)",
                    "0x1537f (in wkernelbase.pdb)",
                    "0x156a6 (in wkernelbase.pdb)"
                ],
                "expected": []
            },
            "no interesting libs": {
                "stack": [
                    "nsPluginNativeWindow::CallSetWindow(nsRefPtr<nsNPAPIPluginInstance> &) (in exciting.pdb)",
                    "nsPluginNativeWindowWin::CallSetWindow(nsRefPtr<nsNPAPIPluginInstance> &) (in exciting.pdb)",
                    "nsObjectFrame::CallSetWindow(bool) (in exciting.pdb)",
                    "nsPluginInstanceOwner::CallSetWindow() (in exciting.pdb)",
                    "nsPluginInstanceOwner::UpdateWindowPositionAndClipRect(bool) (in exciting.pdb)",
                    "nsObjectFrame::SetIsDocumentActive(bool) (in exciting.pdb)",
                    "SetPluginIsActive (in exciting.pdb)",
                    "EnumerateFreezables (in exciting.pdb)",
                    "nsTHashtable<nsBaseHashtableET<nsStringHashKey,`anonymous namespace'::TelemetryIOInterposeObserver::FileStatsByStage> >::s_EnumStub(PLDHashTable *,PLDHashEntryHdr *,unsigned int,void *) (in exciting.pdb)",
                    "PL_DHashTableEnumerate(PLDHashTable *,PLDHashOperator (*)(PLDHashTable *,PLDHashEntryHdr *,unsigned int,void *),void *) (in exciting.pdb)",
                    "nsTHashtable<nsPtrHashKey<nsIContent> >::EnumerateEntries(PLDHashOperator (*)(nsPtrHashKey<nsIContent> *,void *),void *) (in exciting.pdb)",
                    "PresShell::SetIsActive(bool) (in exciting.pdb)",
                    "nsDocShell::SetIsActive(bool) (in exciting.pdb)",
                    "NS_InvokeByIndex (in exciting.pdb)",
                    "XPC_WN_GetterSetter(JSContext *,unsigned int,JS::Value *) (in exciting.pdb)",
                    "js::Invoke(JSContext *,JS::CallArgs,js::MaybeConstruct) (in awesome.pdb)",
                    "js::Invoke(JSContext *,JS::Value const &,JS::Value const &,unsigned int,JS::Value const *,JS::MutableHandle<JS::Value>) (in awesome.pdb)",
                    "js::Shape::set(JSContext *,JS::Handle<JSObject *>,JS::Handle<JSObject *>,bool,JS::MutableHandle<JS::Value>) (in awesome.pdb)",
                    "js::NativeSet<0>(JSContext *,JS::Handle<JSObject *>,JS::Handle<JSObject *>,JS::Handle<js::Shape *>,bool,JS::MutableHandle<JS::Value>) (in awesome.pdb)",
                    "js::baseops::SetPropertyHelper<0>(JSContext *,JS::Handle<JSObject *>,JS::Handle<JSObject *>,JS::Handle<jsid>,js::baseops::QualifiedBool,JS::MutableHandle<JS::Value>,bool) (in awesome.pdb)"
                ],
                "expected": []
            },
            "long stack": {
                "stack": [
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
                ],
                "expected": [
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
                ]
            },
            "irrelevance": {
                "stack": [
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
                ],
                "expected": [
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
            },
            "another long stack": {
                "stack": [
                    "NtCreateFile (in wntdll.pdb)",
                    "CreateFileW (in wkernelbase.pdb)",
                    "CreateFileWImplementation (in wkernel32.pdb)",
                    "File::File(wchar_t const *,File::OpenMode,AccessToken const *) (in DWrite.pdb)",
                    "LocalFileLoader::FontFileStream::FontFileStream(LocalFileLoader *,RefString,DateTime,AccessToken const *,std::pair<void const *,void const *> *) (in DWrite.pdb)",
                    "LocalFileLoader::FontFileStream::CreateFromKey(LocalFileLoader *,void const *,unsigned int,AccessToken const *,std::pair<void const *,void const *> *) (in DWrite.pdb)",
                    "LocalFileLoader::CreateStreamFromKeyInternal(void const *,unsigned int,AccessToken const *,IDWriteFontFileStream * *,std::pair<void const *,void const *> *) (in DWrite.pdb)",
                    "FontFileReference::OpenFileOnDemand() (in DWrite.pdb)",
                    "FontFileReference::GetLastWriteTime() (in DWrite.pdb)",
                    "FontCollectionBuilder::Builder::AddFile(IDWriteFontFile *,IDWriteEudcEnumInfo::EudcFileState,std::basic_string<wchar_t,std::char_traits<wchar_t>,std::allocator<wchar_t>,_STL70> const &) (in DWrite.pdb)",
                    "FontCollectionBuilder::FontCollectionBuilder(IDWriteFactory *,void const *,unsigned int,unsigned __int64,FontLoaderManagers const &,FontCollection const &,CountedPtr<AccessToken> const &) (in DWrite.pdb)",
                    "FontCollectionElement::AddToCacheImpl(FontLoaderManagers const &,CacheWriter &,void const * *,unsigned int *) (in DWrite.pdb)",
                    "CacheWriter::AddElement(FontLoaderManagers const &,IFontCacheElement &,unsigned int,unsigned int,void const * *,unsigned int *,bool *) (in DWrite.pdb)",
                    "ClientSideCacheContext::ClientLookup(IFontCacheElement &,unsigned int,unsigned int) (in DWrite.pdb)",
                    "ClientSideCacheContext::InitializeElementImpl(IFontCacheElement &,unsigned int,unsigned int) (in DWrite.pdb)",
                    "FontCollectionElement::FontCollectionElement(void const *,unsigned int,unsigned __int64,ClientSideCacheContext *,DWriteFactory *,FontCollection const &) (in DWrite.pdb)",
                    "DWriteFontCollection::DWriteFontCollection(void const *,unsigned int,unsigned __int64,ClientSideCacheContext *,DWriteFactory *,FontCollection const &) (in DWrite.pdb)",
                    "ComObject<DWriteFontCollection>::ComObject<DWriteFontCollection><unsigned __int64 *,unsigned int,unsigned __int64,IntrusivePtr<ClientSideCacheContext>,DWriteFactory *,FontCollection>(unsigned __int64 *,unsigned int,unsigned __int64,IntrusivePtr<ClientSideCacheContext>,DWriteFactory *,FontCollection) (in DWrite.pdb)",
                    "InnerComObject<DWriteFactory,DWriteFontCollection>::InnerComObject<DWriteFactory,DWriteFontCollection><unsigned __int64 *,unsigned int,unsigned __int64,IntrusivePtr<ClientSideCacheContext>,DWriteFactory *,FontCollection>(unsigned __int64 *,unsigned int,unsigned __int64,IntrusivePtr<ClientSideCacheContext>,DWriteFactory *,FontCollection) (in DWrite.pdb)",
                    "DWriteFactory::GetSystemFontCollectionInternal(bool) (in DWrite.pdb)",
                    "DWriteFactory::GetSystemFontCollection(IDWriteFontCollection * *,int) (in DWrite.pdb)",
                    "gfxDWriteFontList::DelayedInitFontList() (in xul.pdb)"
                ],
                "expected": [
                    "NtCreateFile (in wntdll.pdb)",
                    "CreateFileW (in wkernelbase.pdb)",
                    "CreateFileWImplementation (in wkernel32.pdb)",
                    "File::File(wchar_t const *,File::OpenMode,AccessToken const *) (in DWrite.pdb)",
                    "LocalFileLoader::FontFileStream::FontFileStream(LocalFileLoader *,RefString,DateTime,AccessToken const *,std::pair<void const *,void const *> *) (in DWrite.pdb)",
                    "LocalFileLoader::FontFileStream::CreateFromKey(LocalFileLoader *,void const *,unsigned int,AccessToken const *,std::pair<void const *,void const *> *) (in DWrite.pdb)",
                    "LocalFileLoader::CreateStreamFromKeyInternal(void const *,unsigned int,AccessToken const *,IDWriteFontFileStream * *,std::pair<void const *,void const *> *) (in DWrite.pdb)",
                    "FontFileReference::OpenFileOnDemand() (in DWrite.pdb)",
                    "FontFileReference::GetLastWriteTime() (in DWrite.pdb)",
                    "FontCollectionBuilder::Builder::AddFile(IDWriteFontFile *,IDWriteEudcEnumInfo::EudcFileState,std::basic_string<wchar_t,std::char_traits<wchar_t>,std::allocator<wchar_t>,_STL70> const &) (in DWrite.pdb)",
                    "FontCollectionBuilder::FontCollectionBuilder(IDWriteFactory *,void const *,unsigned int,unsigned __int64,FontLoaderManagers const &,FontCollection const &,CountedPtr<AccessToken> const &) (in DWrite.pdb)",
                    "FontCollectionElement::AddToCacheImpl(FontLoaderManagers const &,CacheWriter &,void const * *,unsigned int *) (in DWrite.pdb)",
                    "CacheWriter::AddElement(FontLoaderManagers const &,IFontCacheElement &,unsigned int,unsigned int,void const * *,unsigned int *,bool *) (in DWrite.pdb)",
                    "ClientSideCacheContext::ClientLookup(IFontCacheElement &,unsigned int,unsigned int) (in DWrite.pdb)",
                    "ClientSideCacheContext::InitializeElementImpl(IFontCacheElement &,unsigned int,unsigned int) (in DWrite.pdb)",
                    "FontCollectionElement::FontCollectionElement(void const *,unsigned int,unsigned __int64,ClientSideCacheContext *,DWriteFactory *,FontCollection const &) (in DWrite.pdb)",
                    "DWriteFontCollection::DWriteFontCollection(void const *,unsigned int,unsigned __int64,ClientSideCacheContext *,DWriteFactory *,FontCollection const &) (in DWrite.pdb)",
                    "ComObject<DWriteFontCollection>::ComObject<DWriteFontCollection><unsigned __int64 *,unsigned int,unsigned __int64,IntrusivePtr<ClientSideCacheContext>,DWriteFactory *,FontCollection>(unsigned __int64 *,unsigned int,unsigned __int64,IntrusivePtr<ClientSideCacheContext>,DWriteFactory *,FontCollection) (in DWrite.pdb)",
                    "InnerComObject<DWriteFactory,DWriteFontCollection>::InnerComObject<DWriteFactory,DWriteFontCollection><unsigned __int64 *,unsigned int,unsigned __int64,IntrusivePtr<ClientSideCacheContext>,DWriteFactory *,FontCollection>(unsigned __int64 *,unsigned int,unsigned __int64,IntrusivePtr<ClientSideCacheContext>,DWriteFactory *,FontCollection) (in DWrite.pdb)",
                    "DWriteFactory::GetSystemFontCollectionInternal(bool) (in DWrite.pdb)",
                    "DWriteFactory::GetSystemFontCollection(IDWriteFontCollection * *,int) (in DWrite.pdb)",
                    "gfxDWriteFontList::DelayedInitFontList() (in xul.pdb)"
                ]
            },
            "sentinel": {
                "stack": [
                    "MsgWaitForMultipleObjects (in user32.pdb)",
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
                    "nsBaseAppShell::RunSyncSectionsInternal(bool,unsigned int) (in xul.pdb)",
                    "nsThread::ProcessNextEvent(bool,bool *) (in xul.pdb)"
                ],
                "expected": [
                    "mozilla::plugins::PPluginInstanceParent::CallNPP_Destroy(short *) (in xul.pdb)",
                    "mozilla::plugins::PluginInstanceParent::Destroy() (in xul.pdb)",
                    "mozilla::plugins::PluginModuleParent::NPP_Destroy(_NPP *,_NPSavedData * *) (in xul.pdb)",
                    "nsNPAPIPluginInstance::Stop() (in xul.pdb)",
                    "nsPluginHost::StopPluginInstance(nsNPAPIPluginInstance *) (in xul.pdb)",
                    "nsObjectLoadingContent::DoStopPlugin(nsPluginInstanceOwner *,bool,bool) (in xul.pdb)",
                    "nsObjectLoadingContent::StopPluginInstance() (in xul.pdb)",
                    "nsObjectLoadingContent::UnloadObject(bool) (in xul.pdb)",
                    "CheckPluginStopEvent::Run() (in xul.pdb)",
                    "nsBaseAppShell::RunSyncSectionsInternal(bool,unsigned int) (in xul.pdb)",
                    "nsThread::ProcessNextEvent(bool,bool *) (in xul.pdb)"
                ]
            },
            "multiple sentinels": {
                "stack": [
                    'mozilla::plugins::PBrowserStreamChild::Lookup(int) (in xul.pdb)',
                    'mozilla::plugins::PPluginScriptableObjectParent::Read(mozilla::plugins::PPluginScriptableObjectParent * *,IPC::Message const *,void * *,bool) (in xul.pdb)',
                    'mozilla::plugins::PPluginScriptableObjectParent::OnMessageReceived(IPC::Message const &) (in xul.pdb)',
                    'mozilla::plugins::PPluginModuleParent::OnMessageReceived(IPC::Message const &) (in xul.pdb)',
                    'mozilla::ipc::MessageChannel::DispatchAsyncMessage(IPC::Message const &) (in xul.pdb)',
                    'mozilla::ipc::MessageChannel::DispatchMessageW(IPC::Message const &) (in xul.pdb)',
                    'mozilla::ipc::MessageChannel::InterruptCall(IPC::Message *,IPC::Message *) (in xul.pdb)',
                    'mozilla::ipc::MessageChannel::Call(IPC::Message *,IPC::Message *) (in xul.pdb)',
                    'mozilla::plugins::PPluginScriptableObjectParent::CallHasProperty(mozilla::plugins::PPluginIdentifierParent *,bool *) (in xul.pdb)',
                    'mozilla::plugins::PluginScriptableObjectParent::ScriptableHasProperty(NPObject *,void *) (in xul.pdb)',
                    'NPObjWrapper_NewResolve (in xul.pdb)',
                    'JS_GetPropertyById(JSContext *,JS::Handle<JSObject *>,JS::Handle<jsid>,JS::MutableHandle<JS::Value>) (in mozjs.pdb)',
                    'GetProperty (in xul.pdb)',
                    'nsJSObjWrapper::NP_GetProperty(NPObject *,void *,_NPVariant *) (in xul.pdb)',
                    'mozilla::plugins::parent::_getproperty(_NPP *,NPObject *,void *,_NPVariant *) (in xul.pdb)',
                    'mozilla::plugins::PluginScriptableObjectParent::AnswerGetParentProperty(mozilla::plugins::PPluginIdentifierParent *,mozilla::plugins::Variant *,bool *) (in xul.pdb)',
                    'mozilla::plugins::PPluginScriptableObjectParent::OnCallReceived(IPC::Message const &,IPC::Message * &) (in xul.pdb)',
                    'mozilla::plugins::PPluginModuleParent::OnCallReceived(IPC::Message const &,IPC::Message * &) (in xul.pdb)',
                    'mozilla::ipc::MessageChannel::DispatchInterruptMessage(IPC::Message const &,unsigned int) (in xul.pdb)',
                    'mozilla::ipc::MessageChannel::InterruptCall(IPC::Message *,IPC::Message *) (in xul.pdb)',
                    'mozilla::ipc::MessageChannel::Call(IPC::Message *,IPC::Message *) (in xul.pdb)',
                    'mozilla::plugins::PPluginScriptableObjectParent::CallGetChildProperty(mozilla::plugins::PPluginIdentifierParent *,bool *,bool *,mozilla::plugins::Variant *,bool *) (in xul.pdb)',
                    'mozilla::plugins::PluginScriptableObjectParent::GetPropertyHelper(void *,bool *,bool *,_NPVariant *) (in xul.pdb)',
                    'NPObjWrapper_GetProperty (in xul.pdb)',
                    'js::jit::DoGetPropFallback (in mozjs.pdb)',
                    '-0x1',
                    '-0x1',
                    'EnterBaseline (in mozjs.pdb)',
                    'js::jit::EnterBaselineMethod(JSContext *,js::RunState &) (in mozjs.pdb)',
                    'js::RunScript(JSContext *,js::RunState &) (in mozjs.pdb)',
                    'js::Invoke(JSContext *,JS::CallArgs,js::MaybeConstruct) (in mozjs.pdb)',
                    'js_fun_apply(JSContext *,unsigned int,JS::Value *) (in mozjs.pdb)',
                    'js::Invoke(JSContext *,JS::CallArgs,js::MaybeConstruct) (in mozjs.pdb)',
                    'Interpret (in mozjs.pdb)',
                    'js::RunScript(JSContext *,js::RunState &) (in mozjs.pdb)',
                    'js::Invoke(JSContext *,JS::CallArgs,js::MaybeConstruct) (in mozjs.pdb)',
                    'js::Invoke(JSContext *,JS::Value const &,JS::Value const &,unsigned int,JS::Value const *,JS::MutableHandle<JS::Value>) (in mozjs.pdb)',
                    'JS::Call(JSContext *,JS::Handle<JS::Value>,JS::Handle<JS::Value>,JS::HandleValueArray const &,JS::MutableHandle<JS::Value>) (in mozjs.pdb)',
                    'mozilla::dom::Function::Call(JSContext *,JS::Handle<JS::Value>,nsTArray<JS::Value> const &,mozilla::ErrorResult &) (in xul.pdb)',
                    'mozilla::dom::Function::Call<nsCOMPtr<nsISupports> >(nsCOMPtr<nsISupports> const &,nsTArray<JS::Value> const &,mozilla::ErrorResult &,mozilla::dom::CallbackObject::ExceptionHandling) (in xul.pdb)',
                    'nsGlobalWindow::RunTimeoutHandler(nsTimeout *,nsIScriptContext *) (in xul.pdb)',
                    'nsGlobalWindow::RunTimeout(nsTimeout *) (in xul.pdb)',
                    'nsGlobalWindow::TimerCallback(nsITimer *,void *) (in xul.pdb)',
                    'nsTimerImpl::Fire() (in xul.pdb)',
                    'nsTimerEvent::Run() (in xul.pdb)',
                    'nsThread::ProcessNextEvent(bool,bool *) (in xul.pdb)',
                    'NS_ProcessNextEvent(nsIThread *,bool) (in xul.pdb)',
                    'mozilla::ipc::MessagePump::Run(base::MessagePump::Delegate *) (in xul.pdb)',
                    'MessageLoop::RunHandler() (in xul.pdb)',
                    'MessageLoop::Run() (in xul.pdb)',
                    'nsBaseAppShell::Run() (in xul.pdb)',
                    'nsAppShell::Run() (in xul.pdb)',
                    'nsAppStartup::Run() (in xul.pdb)',
                    'XREMain::XRE_mainRun() (in xul.pdb)',
                    'XREMain::XRE_main(int,char * * const,nsXREAppData const *) (in xul.pdb)',
                    'XRE_main (in xul.pdb)',
                    'do_main (in firefox.pdb)',
                    'NS_internal_main(int,char * *) (in firefox.pdb)',
                    'wmain (in firefox.pdb)',
                    '__tmainCRTStartup (in firefox.pdb)',
                    'BaseThreadInitThunk (in kernel32.pdb)',
                    '__RtlUserThreadStart (in ntdll.pdb)',
                    '_RtlUserThreadStart (in ntdll.pdb)'
                ],
                "expected": [
                    'mozilla::plugins::PPluginScriptableObjectParent::CallHasProperty(mozilla::plugins::PPluginIdentifierParent *,bool *) (in xul.pdb)',
                    'mozilla::plugins::PluginScriptableObjectParent::ScriptableHasProperty(NPObject *,void *) (in xul.pdb)',
                    'NPObjWrapper_NewResolve (in xul.pdb)',
                    'JS_GetPropertyById(JSContext *,JS::Handle<JSObject *>,JS::Handle<jsid>,JS::MutableHandle<JS::Value>) (in mozjs.pdb)',
                    'GetProperty (in xul.pdb)',
                    'nsJSObjWrapper::NP_GetProperty(NPObject *,void *,_NPVariant *) (in xul.pdb)',
                    'mozilla::plugins::parent::_getproperty(_NPP *,NPObject *,void *,_NPVariant *) (in xul.pdb)',
                    'mozilla::plugins::PluginScriptableObjectParent::AnswerGetParentProperty(mozilla::plugins::PPluginIdentifierParent *,mozilla::plugins::Variant *,bool *) (in xul.pdb)',
                    'mozilla::plugins::PPluginScriptableObjectParent::OnCallReceived(IPC::Message const &,IPC::Message * &) (in xul.pdb)',
                    'mozilla::plugins::PPluginModuleParent::OnCallReceived(IPC::Message const &,IPC::Message * &) (in xul.pdb)',
                    'mozilla::ipc::MessageChannel::DispatchInterruptMessage(IPC::Message const &,unsigned int) (in xul.pdb)',
                    'mozilla::ipc::MessageChannel::InterruptCall(IPC::Message *,IPC::Message *) (in xul.pdb)',
                    'mozilla::ipc::MessageChannel::Call(IPC::Message *,IPC::Message *) (in xul.pdb)',
                    'mozilla::plugins::PPluginScriptableObjectParent::CallGetChildProperty(mozilla::plugins::PPluginIdentifierParent *,bool *,bool *,mozilla::plugins::Variant *,bool *) (in xul.pdb)',
                    'mozilla::plugins::PluginScriptableObjectParent::GetPropertyHelper(void *,bool *,bool *,_NPVariant *) (in xul.pdb)'
                ]
            },
            "old stack": {
                "stack": [
                    "KiFastSystemCallRet (in ntdll.pdb)",
                    "WaitForMultipleObjectsExImplementation (in kernel32.pdb)",
                    "RealMsgWaitForMultipleObjectsEx (in user32.pdb)",
                    "MsgWaitForMultipleObjects (in user32.pdb)",
                    "0x345f94 (in xul.pdb)",
                    "0x34e42e (in xul.pdb)",
                    "0x34ec03 (in xul.pdb)",
                    "0x3ce6c5 (in xul.pdb)",
                    "0x534f96 (in xul.pdb)",
                    "0xc6ae55 (in xul.pdb)",
                    "0xce0925 (in xul.pdb)",
                    "0xce0a82 (in xul.pdb)",
                    "0xe2c8e3 (in xul.pdb)",
                    "0xe2c96c (in xul.pdb)",
                    "0xede5f5 (in xul.pdb)",
                    "0xf94a79 (in xul.pdb)",
                    "0x104e80c (in xul.pdb)",
                    "0x104ecff (in xul.pdb)",
                    "0xb0f45a (in xul.pdb)",
                    "0xb4720 (in xul.pdb)",
                    "0xd1c4d (in xul.pdb)",
                    "0x3495fb (in xul.pdb)",
                    "0x33ce45 (in xul.pdb)",
                    "0x33d36a (in xul.pdb)",
                    "0x29d7f0 (in xul.pdb)",
                    "0x2a4029 (in xul.pdb)",
                    "0x8a5600 (in xul.pdb)",
                    "0x22a93b (in xul.pdb)",
                    "0x28b5f1 (in xul.pdb)",
                    "0x2a790d (in xul.pdb)",
                    "0x19c2 (in firefox.pdb)",
                    "0x201c (in firefox.pdb)",
                    "0x2126 (in firefox.pdb)",
                    "0x2a76 (in firefox.pdb)",
                    "BaseThreadInitThunk (in kernel32.pdb)",
                    "__RtlUserThreadStart (in ntdll.pdb)",
                    "_RtlUserThreadStart (in ntdll.pdb)"
                ],
                "expected": []
            },
            "": {
                "stack": [
                ],
                "expected": [
                ]
            }
        }

    def run_one_test(self, key):
        test = self.get_test_stacks()[key]
        test_stack = test["stack"]
        expected = test["expected"]
        actual = symbolicate.get_signature(test_stack)
        self.assertEqual(len(expected), len(actual))
        self.assertEqual(expected, actual)

    def test_signature(self):
        self.run_one_test("normal signature")

    def test_completely_boring_stack(self):
        self.run_one_test("completely boring")

    def test_no_interesting_libs(self):
        self.run_one_test("no interesting libs")

    def test_long_sig(self):
        self.run_one_test("long stack")

    def test_another_long_sig(self):
        self.run_one_test("another long stack")

    def test_irrelevance(self):
        # RealMsgWaitForMultipleObjectsEx (in wuser32.pdb) should be filtered as an irrelevant frame
        self.run_one_test("irrelevance")

    def test_sentinel(self):
        self.run_one_test("sentinel")

    def test_multiple_sentinels(self):
        self.run_one_test("multiple sentinels")

    def test_old_stack(self):
        self.run_one_test("old stack")

    def test_combine_stacks(self):
        test = {
            ("a", "b", 1): 9,
            ("a", "b", 2): 10,
            ("c", "d", 5): 11
        }
        expected = {
            ("a", "b"): [1, 2],
            ("c", "d"): [5]
        }
        actual = symbolicate.combine_stacks(test)
        e_ab = expected[("a", "b")]
        e_cd = expected[("c", "d")]
        a_ab = actual[("a", "b")]
        a_cd = actual[("c", "d")]

        # Order isn't important, but make sure we have all the same elements.
        self.assertEqual(len(e_ab), len(a_ab))
        self.assertEqual(len(e_cd), len(a_cd))
        for i in e_ab:
            self.assertIn(i, a_ab)
        for i in e_cd:
            self.assertIn(i, a_cd)

    def test_get_stack_key(self):
        entry = [0, 100]
        mmap = [["test.pdb", "DEBUG_ID"]]
        expected = ("test.pdb", "DEBUG_ID", 100)
        self.assertEqual(expected, symbolicate.get_stack_key(entry, mmap))

        # bogus mmap index:
        self.assertIs(symbolicate.get_stack_key([5, 100], mmap), None)

        # negative mmap index:
        self.assertIs(symbolicate.get_stack_key([-1, 100], mmap), None)

if __name__ == "__main__":
    unittest.main()
