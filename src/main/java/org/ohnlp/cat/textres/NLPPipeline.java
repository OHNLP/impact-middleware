package org.ohnlp.cat.textres;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.internal.ResourceManagerFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.InvalidXMLException;
import org.ohnlp.cat.ApplicationConfiguration;
import org.ohnlp.medtagger.backbone.MedTaggerBackboneTransform;
import org.ohnlp.medtagger.context.RuleContextAnnotator;
import org.ohnlp.medtagger.type.ConceptMention;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class NLPPipeline {
    private static final ReentrantLock INIT_MUTEX_LOCK = new ReentrantLock(); // Lock to ensure init once per thread

    private final ApplicationConfiguration config;
    private ResourceManager resMgr;
    private AnalysisEngine aae;
    private CAS cas;

    public NLPPipeline(ApplicationConfiguration config) {
        this.config = config;
        init();
    }

    private void init() {
        try {
            INIT_MUTEX_LOCK.lock();
            AggregateBuilder ae = new AggregateBuilder();
            // Tokenization, Sentence Splitting, Section Detection, etc.
            ae.add(createEngineDescription("desc.backbone.aes.PreConceptExtractionAE"));
            // Add the UMLS dictionary
            URI localUI = NLPPipeline.class.getResource("/medtaggerresources/dict/umls.dict").toURI();
            ae.add(createEngineDescription("desc.backbone.aes.MedTaggerDictionaryLookupAE", "dict_file", localUI.toString()));
            // Add Context handling
            ae.add(AnalysisEngineFactory.createEngineDescription(RuleContextAnnotator.class));
            this.resMgr = ResourceManagerFactory.newResourceManager();
            this.aae = UIMAFramework.produceAnalysisEngine(ae.createAggregateDescription(), resMgr, null);
            this.cas = CasCreationUtils.createCas(Collections.singletonList(aae.getMetaData()),
                    null, resMgr);
        } catch (ResourceInitializationException | InvalidXMLException | IOException | URISyntaxException e) {
            throw new RuntimeException("Failed to init text/NLP parser");
        } finally {
            INIT_MUTEX_LOCK.unlock();
        }
    }

    public Set<String> process(String text) {
        try {
            cas.reset();
        } catch (Throwable t) {
            // CAS reset failed, just create a whole new one
            try {
                this.cas = CasCreationUtils.createCas(Collections.singletonList(aae.getMetaData()),
                        null, resMgr);
            } catch (ResourceInitializationException e) {
                throw new RuntimeException(e);
            }
        }
        cas.setDocumentText(text);
        final CAS casRef = cas; // a final reference for cross-ref access
        System.out.println("Running NLP on " + text);
        // Run NLP in a separate thread so that we can interrupt it if it takes too long
        // We create a new executor service instead of sharing it in case interrupt fails
        ExecutorService nlpExecutor = Executors.newSingleThreadExecutor();
        FutureTask<Throwable> future = new FutureTask<>(() -> {
            try {
                aae.process(casRef);
                return null;
            } catch (AnalysisEngineProcessException e) {
                return e;
            }
        });
        nlpExecutor.submit(future);
        try {
            Throwable t = future.get(30000, TimeUnit.MILLISECONDS);
            if (t != null) {
                throw new RuntimeException(t);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.out.println("Skipping text " + text + " due to NLP run taking longer than 30 seconds");
            future.cancel(true);
            nlpExecutor.shutdownNow();
            throw new RuntimeException(e);
        } catch (Throwable t) {
            System.out.println("Skipping text " + text + " due to error. Note that this may be expected if NLP run " +
                    "takes longer than a certain amount of time and it short circuits");
            t.printStackTrace();
            future.cancel(true);
            nlpExecutor.shutdownNow();
            throw new RuntimeException(t);
        }
        try {
            JCas jcas = casRef.getJCas();
            Set<String> ret = new HashSet<>();
            for (ConceptMention cm : JCasUtil.select(jcas, ConceptMention.class)) {
                ret.add(cm.getNormTarget());
            }
            return ret;
        } catch (CASException e) {
            throw new RuntimeException(e);
        }
    }
}
