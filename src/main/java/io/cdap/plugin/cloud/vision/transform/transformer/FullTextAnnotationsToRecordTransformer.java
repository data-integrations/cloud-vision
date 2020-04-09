/*
 * Copyright © 2019 Cask Data, Inc.
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
 */

package io.cdap.plugin.cloud.vision.transform.transformer;

import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.Block;
import com.google.cloud.vision.v1.Page;
import com.google.cloud.vision.v1.Paragraph;
import com.google.cloud.vision.v1.Symbol;
import com.google.cloud.vision.v1.TextAnnotation;
import com.google.cloud.vision.v1.Word;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cloud.vision.transform.schema.ColorInfoSchema;
import io.cdap.plugin.cloud.vision.transform.schema.FullTextAnnotationSchema;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Transforms handwriting annotation of specified {@link AnnotateImageResponse} to {@link StructuredRecord} according
 * to the specified schema.
 */
public class FullTextAnnotationsToRecordTransformer extends ImageAnnotationToRecordTransformer {

  public FullTextAnnotationsToRecordTransformer(Schema schema, String outputFieldName) {
    super(schema, outputFieldName);
  }

  /**
   * Extract the entire mapping of a {@link AnnotateImageResponse} object to a {@link StructuredRecord}
   * using the {@link FullTextAnnotationSchema}. This {@link StructuredRecord} can then be turned into a json document.
   *
   * @param input                 {@link StructuredRecord} to add to.
   * @param annotateImageResponse {@link AnnotateImageResponse} to get the data from.
   */
  @Override
  public StructuredRecord transform(StructuredRecord input, AnnotateImageResponse annotateImageResponse) {
    TextAnnotation annotation = annotateImageResponse.getFullTextAnnotation();
    return getOutputRecordBuilder(input)
            .set(outputFieldName, extractHandwritingAnnotation(annotation))
            .build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the handwriting information from a {@link TextAnnotation} input
   * using a {@link io.cdap.plugin.cloud.vision.transform.schema.TextAnnotationSchema}.
   *
   * @param annotation A {@link TextAnnotation} object containing the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractHandwritingAnnotation(TextAnnotation annotation) {
    Schema hwSchema = getHandwritingAnnotationSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(hwSchema);
    if (hwSchema.getField(FullTextAnnotationSchema.TEXT_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.TEXT_FIELD_NAME, annotation.getText());
    }

    Schema.Field pagesField = hwSchema.getField(FullTextAnnotationSchema.PAGES_FIELD_NAME);
    if (pagesField != null) {
      Schema pageSchema = getComponentSchema(pagesField);
      List<StructuredRecord> pages = annotation.getPagesList().stream()
              .map(v -> extractPage(v, pageSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.PAGES_FIELD_NAME, pages);
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the page information from a {@link Page} input
   * using a {@link Schema} for the mapping.
   *
   * @param page   The {@link Page} object containing the data.
   * @param schema The {@link Schema} to use for the mapping of the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractPage(Page page, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(FullTextAnnotationSchema.TextPage.TEXT_FIELD_NAME) != null) {
      String pageText = page.getBlocksList().stream()
              .map(Block::getParagraphsList)
              .flatMap(List::stream)
              .map(Paragraph::getWordsList)
              .flatMap(List::stream)
              .map(Word::getSymbolsList)
              .flatMap(List::stream)
              .map(Symbol::getText)
              .collect(Collectors.joining());
      builder.set(FullTextAnnotationSchema.TextPage.TEXT_FIELD_NAME, pageText);
    }
    if (schema.getField(FullTextAnnotationSchema.TextPage.WIDTH_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.TextPage.WIDTH_FIELD_NAME, page.getWidth());
    }
    if (schema.getField(FullTextAnnotationSchema.TextPage.HEIGHT_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.TextPage.HEIGHT_FIELD_NAME, page.getHeight());
    }
    if (schema.getField(FullTextAnnotationSchema.TextPage.CONFIDENCE_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.TextPage.CONFIDENCE_FIELD_NAME, page.getConfidence());
    }
    Schema.Field languagesField = schema.getField(FullTextAnnotationSchema.TextPage.DETECTED_LANGUAGES_FIELD_NAME);
    if (languagesField != null) {
      Schema langSchema = getComponentSchema(languagesField);
      List<StructuredRecord> languages = page.getProperty().getDetectedLanguagesList().stream()
              .map(l -> extractDetectedLanguage(l, langSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextPage.DETECTED_LANGUAGES_FIELD_NAME, languages);
    }
    if (schema.getField(FullTextAnnotationSchema.TextPage.DETECTED_BREAK_FIELD_NAME) != null) {
      String detectedBreak = page.getProperty().getDetectedBreak().getType().name();
      builder.set(FullTextAnnotationSchema.TextPage.DETECTED_BREAK_FIELD_NAME, detectedBreak);
    }
    Schema.Field blocksField = schema.getField(FullTextAnnotationSchema.TextPage.BLOCKS_FIELD_NAME);
    if (blocksField != null) {
      Schema blockSchema = getComponentSchema(blocksField);
      List<StructuredRecord> blocks = page.getBlocksList().stream()
              .map(v -> extractBlock(v, blockSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextPage.BLOCKS_FIELD_NAME, blocks);
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the block information from a {@link Block} input
   * using a {@link Schema} for the mapping.
   *
   * @param block  The {@link Block} object containing the data.
   * @param schema The {@link Schema} to use for the mapping of the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractBlock(Block block, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(FullTextAnnotationSchema.TextBlock.TEXT_FIELD_NAME) != null) {
      String blockText = block.getParagraphsList().stream()
              .map(Paragraph::getWordsList)
              .flatMap(List::stream)
              .map(Word::getSymbolsList)
              .flatMap(List::stream)
              .map(Symbol::getText)
              .collect(Collectors.joining());
      builder.set(FullTextAnnotationSchema.TextBlock.TEXT_FIELD_NAME, blockText);
    }
    if (schema.getField(FullTextAnnotationSchema.TextBlock.BLOCK_TYPE_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.TextBlock.BLOCK_TYPE_FIELD_NAME, block.getBlockType().name());
    }
    if (schema.getField(FullTextAnnotationSchema.TextBlock.CONFIDENCE_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.TextBlock.CONFIDENCE_FIELD_NAME, block.getConfidence());
    }
    Schema.Field languagesField = schema.getField(FullTextAnnotationSchema.TextBlock.DETECTED_LANGUAGES_FIELD_NAME);
    if (languagesField != null) {
      Schema langSchema = getComponentSchema(languagesField);
      List<StructuredRecord> languages = block.getProperty().getDetectedLanguagesList().stream()
              .map(l -> extractDetectedLanguage(l, langSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextBlock.DETECTED_LANGUAGES_FIELD_NAME, languages);
    }
    if (schema.getField(FullTextAnnotationSchema.TextBlock.DETECTED_BREAK_FIELD_NAME) != null) {
      String detectedBreak = block.getProperty().getDetectedBreak().getType().name();
      builder.set(FullTextAnnotationSchema.TextBlock.DETECTED_BREAK_FIELD_NAME, detectedBreak);
    }
    Schema.Field paragraphsField = schema.getField(FullTextAnnotationSchema.TextBlock.PARAGRAPHS_FIELD_NAME);
    if (paragraphsField != null) {
      Schema paragraphSchema = getComponentSchema(paragraphsField);
      List<StructuredRecord> paragraphs = block.getParagraphsList().stream()
              .map(v -> extractParagraph(v, paragraphSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextBlock.PARAGRAPHS_FIELD_NAME, paragraphs);
    }
    Schema.Field boxField = schema.getField(FullTextAnnotationSchema.TextBlock.BOUNDING_BOX_FIELD_NAME);
    if (boxField != null) {
      Schema vertexSchema = getComponentSchema(boxField);
      List<StructuredRecord> paragraphs = block.getBoundingBox().getVerticesList().stream()
              .map(v -> extractVertex(v, vertexSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextBlock.BOUNDING_BOX_FIELD_NAME, paragraphs);
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the paragraph information from a {@link Paragraph} input
   * using a {@link Schema} for the mapping.
   *
   * @param paragraph The {@link Paragraph} object containing the data.
   * @param schema    The {@link Schema} to use for the mapping of the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractParagraph(Paragraph paragraph, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(FullTextAnnotationSchema.TextParagraph.TEXT_FIELD_NAME) != null) {
      String paragraphText = paragraph.getWordsList().stream()
              .map(Word::getSymbolsList)
              .flatMap(List::stream)
              .map(Symbol::getText)
              .collect(Collectors.joining());
      builder.set(FullTextAnnotationSchema.TextParagraph.TEXT_FIELD_NAME, paragraphText);
    }
    if (schema.getField(FullTextAnnotationSchema.TextParagraph.CONFIDENCE_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.TextParagraph.CONFIDENCE_FIELD_NAME, paragraph.getConfidence());
    }
    Schema.Field languagesField = schema.getField(FullTextAnnotationSchema.TextParagraph.DETECTED_LANGUAGES_FIELD_NAME);
    if (languagesField != null) {
      Schema langSchema = getComponentSchema(languagesField);
      List<StructuredRecord> languages = paragraph.getProperty().getDetectedLanguagesList().stream()
              .map(l -> extractDetectedLanguage(l, langSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextParagraph.DETECTED_LANGUAGES_FIELD_NAME, languages);
    }
    if (schema.getField(FullTextAnnotationSchema.TextParagraph.DETECTED_BREAK_FIELD_NAME) != null) {
      String detectedBreak = paragraph.getProperty().getDetectedBreak().getType().name();
      builder.set(FullTextAnnotationSchema.TextParagraph.DETECTED_BREAK_FIELD_NAME, detectedBreak);
    }
    Schema.Field wordsField = schema.getField(FullTextAnnotationSchema.TextParagraph.WORDS_FIELD_NAME);
    if (wordsField != null) {
      Schema wordSchema = getComponentSchema(wordsField);
      List<StructuredRecord> words = paragraph.getWordsList().stream()
              .map(v -> extractWord(v, wordSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextParagraph.WORDS_FIELD_NAME, words);
    }
    Schema.Field boxField = schema.getField(FullTextAnnotationSchema.TextParagraph.BOUNDING_BOX_FIELD_NAME);
    if (boxField != null) {
      Schema vertexSchema = getComponentSchema(boxField);
      List<StructuredRecord> paragraphs = paragraph.getBoundingBox().getVerticesList().stream()
              .map(v -> extractVertex(v, vertexSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextParagraph.BOUNDING_BOX_FIELD_NAME, paragraphs);
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the paragraph information from a {@link Word} input
   * using a {@link Schema} for the mapping.
   *
   * @param word   The {@link Word} object containing the data.
   * @param schema The {@link Schema} to use for the mapping of the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractWord(Word word, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(FullTextAnnotationSchema.TextWord.TEXT_FIELD_NAME) != null) {
      String wordText = word.getSymbolsList().stream()
              .map(Symbol::getText)
              .collect(Collectors.joining());
      builder.set(FullTextAnnotationSchema.TextWord.TEXT_FIELD_NAME, wordText);
    }
    if (schema.getField(FullTextAnnotationSchema.TextWord.CONFIDENCE_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.TextWord.CONFIDENCE_FIELD_NAME, word.getConfidence());
    }
    Schema.Field languagesField = schema.getField(FullTextAnnotationSchema.TextWord.DETECTED_LANGUAGES_FIELD_NAME);
    if (languagesField != null) {
      Schema langSchema = getComponentSchema(languagesField);
      List<StructuredRecord> languages = word.getProperty().getDetectedLanguagesList().stream()
              .map(l -> extractDetectedLanguage(l, langSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextWord.DETECTED_LANGUAGES_FIELD_NAME, languages);
    }
    if (schema.getField(FullTextAnnotationSchema.TextWord.DETECTED_BREAK_FIELD_NAME) != null) {
      String detectedBreak = word.getProperty().getDetectedBreak().getType().name();
      builder.set(FullTextAnnotationSchema.TextWord.DETECTED_BREAK_FIELD_NAME, detectedBreak);
    }
    Schema.Field symbolsField = schema.getField(FullTextAnnotationSchema.TextWord.SYMBOLS_FIELD_NAME);
    if (symbolsField != null) {
      Schema symbolSchema = getComponentSchema(symbolsField);
      List<StructuredRecord> words = word.getSymbolsList().stream()
              .map(v -> extractSymbol(v, symbolSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextWord.SYMBOLS_FIELD_NAME, words);
    }
    Schema.Field boxField = schema.getField(FullTextAnnotationSchema.TextWord.BOUNDING_BOX_FIELD_NAME);
    if (boxField != null) {
      Schema vertexSchema = getComponentSchema(boxField);
      List<StructuredRecord> paragraphs = word.getBoundingBox().getVerticesList().stream()
              .map(v -> extractVertex(v, vertexSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextWord.BOUNDING_BOX_FIELD_NAME, paragraphs);
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the paragraph information from a {@link Symbol} input
   * using a {@link Schema} for the mapping.
   *
   * @param symbol The {@link Symbol} object containing the data.
   * @param schema The {@link Schema} to use for the mapping of the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractSymbol(Symbol symbol, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(FullTextAnnotationSchema.TextSymbol.TEXT_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.TextSymbol.TEXT_FIELD_NAME, symbol.getText());
    }
    if (schema.getField(FullTextAnnotationSchema.TextSymbol.CONFIDENCE_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.TextSymbol.CONFIDENCE_FIELD_NAME, symbol.getConfidence());
    }
    Schema.Field languagesField = schema.getField(FullTextAnnotationSchema.TextSymbol.DETECTED_LANGUAGES_FIELD_NAME);
    if (languagesField != null) {
      Schema langSchema = getComponentSchema(languagesField);
      List<StructuredRecord> languages = symbol.getProperty().getDetectedLanguagesList().stream()
              .map(l -> extractDetectedLanguage(l, langSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextSymbol.DETECTED_LANGUAGES_FIELD_NAME, languages);
    }
    if (schema.getField(FullTextAnnotationSchema.TextSymbol.DETECTED_BREAK_FIELD_NAME) != null) {
      String detectedBreak = symbol.getProperty().getDetectedBreak().getType().name();
      builder.set(FullTextAnnotationSchema.TextSymbol.DETECTED_BREAK_FIELD_NAME, detectedBreak);
    }
    Schema.Field boxField = schema.getField(FullTextAnnotationSchema.TextSymbol.BOUNDING_BOX_FIELD_NAME);
    if (boxField != null) {
      Schema vertexSchema = getComponentSchema(boxField);
      List<StructuredRecord> paragraphs = symbol.getBoundingBox().getVerticesList().stream()
              .map(v -> extractVertex(v, vertexSchema))
              .collect(Collectors.toList());
      builder.set(FullTextAnnotationSchema.TextSymbol.BOUNDING_BOX_FIELD_NAME, paragraphs);
    }

    return builder.build();
  }

  /**
   * Extract a {@link StructuredRecord} containing the detected language information from a
   * {@link TextAnnotation.DetectedLanguage} input using a {@link Schema} for the mapping.
   *
   * @param language The {@link Symbol} object containing the data.
   * @param schema   The {@link Schema} to use for the mapping of the data.
   * @return A {@link StructuredRecord} containing the data mapped.
   */
  private StructuredRecord extractDetectedLanguage(TextAnnotation.DetectedLanguage language, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField(FullTextAnnotationSchema.DetectedLanguage.LANGUAGE_CODE_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.DetectedLanguage.LANGUAGE_CODE_FIELD_NAME, language.getLanguageCode());
    }
    if (schema.getField(FullTextAnnotationSchema.DetectedLanguage.CONFIDENCE_FIELD_NAME) != null) {
      builder.set(FullTextAnnotationSchema.DetectedLanguage.CONFIDENCE_FIELD_NAME, language.getConfidence());
    }

    return builder.build();
  }

  /**
   * Retrieves Handwriting Annotation's non-nullable component schema. Handwriting Annotation's schema is retrieved
   * instead of using constant schema since users are free to choose to not include some of the fields.
   *
   * @return Handwriting Annotation's non-nullable component schema.
   */
  private Schema getHandwritingAnnotationSchema() {
    Schema handwritingAnnotationsFieldSchema = schema.getField(outputFieldName).getSchema();
    return handwritingAnnotationsFieldSchema.isNullable() ? handwritingAnnotationsFieldSchema.getNonNullable()
            : handwritingAnnotationsFieldSchema;
  }
}
