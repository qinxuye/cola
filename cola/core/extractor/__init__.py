#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Copyright (c) 2013 Qin Xuye <qin@qinxuye.me>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Created on 2013-6-16

@author: Chine
'''

import re

from cola.core.logs import get_logger
from cola.core.errors import DependencyNotInstalledError

try:
    from bs4 import NavigableString
except ImportError:
    raise DependencyNotInstalledError("BeautifulSoup4")

from cola.core.extractor.preprocess import PreProcessor
from cola.core.extractor.utils import beautiful_soup

__all__ = ['Extractor']

REGEXES = { 
    'unlikelyCandidatesRe': re.compile('combx|comment|disqus|foot|header|menu|meta|nav|rss|shoutbox|sidebar|aside|sponsor',re.I),
    'okMaybeItsACandidateRe': re.compile('and|article|body|column|main',re.I),
    'positiveRe': re.compile('article|body|content|entry|hentry|page|pagination|post|text',re.I),
    'negativeRe': re.compile('combx|comment|contact|foot|footer|footnote|link|media|meta|promo|related|scroll|shoutbox|sponsor|tags|widget',re.I),
    'divToPElementsRe': re.compile('<(a|blockquote|dl|div|img|ol|p|pre|table|ul)',re.I),
    'replaceBrsRe': re.compile('(<br[^>]*>[ \n\r\t]*){2,}',re.I),
    'replaceFontsRe': re.compile('<(\/?)font[^>]*>',re.I),
    'trimRe': re.compile('^\s+|\s+$/'),
    'normalizeRe': re.compile('\s{2,}/'),
    'killBreaksRe': re.compile('(<br\s*\/?>(\s|&nbsp;?)*){1,}/'),
    'videoRe': re.compile('http:\/\/(www\.)?(youtube|vimeo)\.com', re.I),
}

class HashableElement():
    def __init__(self, node):
        self.node = node
        self._path = None

    def _get_path(self):
        if self._path is None:
            reverse_path = []
            node = self.node
            while node:
                node_id = (node.name, tuple(node.attrs), node.string)
                reverse_path.append(node_id)
                node = node.parent
            self._path = tuple(reverse_path)
        return self._path
    path = property(_get_path)

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        return self.path == other.path

    def __getattr__(self, name):
        return getattr(self.node, name)

class Extractor(object):
    TEXT_LENGTH_THRESHOLD = 25
    RETRY_LENGTH = 250
    
    def __init__(self, content, base_url=None, logger=None, debug=False, **options):
        self._content = content
        self.logger = logger
        self.base_url = base_url
        if self.logger is None:
            self.logger = get_logger('cola_extractor')
        self.on_debug = debug
        self.debug = self.logger.info if debug else (lambda s: None)
        self.options = options
            
        self._title = None
        self._html = None
            
    def preprocess(self, force=False):
        if force is True or self._html is None:
            preprocessor = PreProcessor(self._content, base_url=self.base_url)
            self._title, self._html = preprocessor.process()
            
    def title(self, force=False):
        self.preprocess(force=force)
        return self._title
    
    def content(self, force=False):
        self.preprocess(force=force)
        return self._html
    
    def _tags(self, node, *tag_names):
        for tag_name in tag_names:
            for n in node.find_all(tag_name):
                yield n
                
    def _text(self, node):
        return ''.join(node.find_all(text=True))
    
    def _describe(self, node):
        if not hasattr(node, 'name'):
            return "[text]"
        return "%s#%s.%s" % (
            node.name, node.get('id', ''), node.get('class',''))
                
    def _remove_unlikely_candidates(self):
        for elem in self._html.find_all():
            s = '%s%s%s' % (
                elem.name, elem.get('class', ''), elem.get('id', '')
            )
            if REGEXES['unlikelyCandidatesRe'].search(s) and \
                (not REGEXES['okMaybeItsACandidateRe'].search(s)) and \
                elem.name != 'body':
                self.debug("Removing unlikely candidate - %s" % (s,))
                elem.extract()
                
    def _transform_misused_divs_into_p(self):
        for elem in self._html.find_all('div'):
            if not REGEXES['divToPElementsRe'].search(''.join(map(unicode, elem.contents))):
                self.debug("Altering div(#%s.%s) to p" % (elem.get('id', ''), elem.get('class', '')))
                elem.name = 'p'
                
    def _get_link_density(self, node):
        link_length = len("".join([i.text or "" for i in node.find_all("a")]))
        text_length = len(self._text(node))
        return float(link_length) / max(text_length, 1)
                
    def _weight_node(self, node):
        weight = 0
        if node.get('class', None):
            cls = ''.join(node['class'])
            
            if REGEXES['negativeRe'].search(cls):
                weight -= 25

            if REGEXES['positiveRe'].search(cls):
                weight += 25

        if node.get('id', None):
            if REGEXES['negativeRe'].search(node['id']):
                weight -= 25

            if REGEXES['positiveRe'].search(node['id']):
                weight += 25

        return weight
                
    def _score_node(self, node):
        content_score = self._weight_node(node)
        name = node.name.lower()
        if name in ("div", "article"):
            content_score += 5
        elif name == "blockquote":
            content_score += 3
        elif name == "form":
            content_score -= 3
        elif name == "th":
            content_score -= 5
        return { 'content_score': content_score, 'elem': node }
                
    def _score_paragraphs(self, min_text_length=None):
        if min_text_length is None:
            min_text_length = self.TEXT_LENGTH_THRESHOLD
            
        candidates = {}
        elems = self._tags(self._html, 'p', 'td')
        
        for elem in elems:
            parent_node = elem.parent
            grand_parent_node = parent_node.parent
            parent_key = HashableElement(parent_node)
            grand_parent_key = HashableElement(grand_parent_node)

            inner_text = self._text(elem)
            
            # If this paragraph is less than 25 characters, don't even count it.
            if (not inner_text) or len(inner_text) < min_text_length:
                continue
            
            if parent_key not in candidates:
                candidates[parent_key] = self._score_node(parent_node)
            if grand_parent_node and grand_parent_key not in candidates:
                candidates[grand_parent_key] = self._score_node(grand_parent_node)
                
            content_score = 1
            content_score += len(re.split(ur',|，', inner_text))
            content_score += min([(len(inner_text) / 100), 3])

            candidates[parent_key]['content_score'] += content_score
            if grand_parent_node:
                candidates[grand_parent_key]['content_score'] += content_score / 2.0
                
        # Scale the final candidates score based on link density. Good content should have a
        # relatively small link density (5% or less) and be mostly unaffected by this operation.
        for elem, candidate in candidates.items():
            candidate['content_score'] *= (1 - self._get_link_density(elem))
            self.debug("candidate %s scored %s" % (self._describe(elem), candidate['content_score']))

        return candidates
    
    def _select_best_candidate(self, candidates):
        sorted_candidates = sorted(candidates.values(), 
                                   key=lambda x: x['content_score'], 
                                   reverse=True)
        self.debug("Top 5 candidates:")
        for candidate in sorted_candidates[:5]:
            elem = candidate['elem']
            self.debug("Candidate %s with score %s" % \
                       (self._describe(elem), candidate['content_score']))

        if len(sorted_candidates) == 0:
            return None
        best_candidate = sorted_candidates[0]
        self.debug("Best candidate %s with score %s" % \
                   (self._describe(best_candidate['elem']), best_candidate['content_score']))
        return best_candidate
    
    def _get_article(self, candidates, best_candidate):
        # Now that we have the top candidate, look through its siblings for content that might also be related.
        # Things like preambles, content split by ads that we removed, etc.
        
        sibling_score_threshold = max([10, best_candidate['content_score'] * 0.2])
        output = beautiful_soup("<div/>")
        for sibling in best_candidate['elem'].parent.contents:
            if isinstance(sibling, NavigableString): continue
            append = False
            if sibling is best_candidate['elem']:
                append = True
            sibling_key = HashableElement(sibling)
            if sibling_key in candidates and \
                candidates[sibling_key]['content_score'] >= sibling_score_threshold:
                append = True

            if sibling.name == "p":
                link_density = self._get_link_density(sibling)
                node_content = sibling.string or ""
                node_length = len(node_content)

                if node_length > 80 and link_density < 0.25:
                    append = True
                elif node_length < 80 and link_density == 0 and re.search('\.( |$)', node_content):
                    append = True

            if append:
                output.div.append(sibling)
                
        return output
    
    def _sanitize(self, node, candidates):
        for header in self._tags(node, "h1", "h2", "h3", "h4", "h5", "h6"):
            if self._weight_node(header) < 0 or \
                self._get_link_density(header) > 0.33: 
                header.extract()

        for elem in self._tags(node, "form", "iframe"):
            elem.extract()

        # Conditionally clean <table>s, <ul>s, and <div>s
        for el in self._tags(node, "table", "ul", "div"):
            weight = self._weight_node(el)
            el_key = HashableElement(el)
            if el_key in candidates:
                content_score = candidates[el_key]['content_score']
            else:
                content_score = 0
            name = el.name

            if weight + content_score < 0:
                el.extract()
                self.debug("Conditionally cleaned %s with weight %s and content score %s because score + content score was less than zero." %
                    (self._describe(el), weight, content_score))
            elif len(re.split(ur',|，', self._text(el))) < 10:
                counts = {}
                for kind in ['p', 'img', 'li', 'a', 'embed', 'input']:
                    counts[kind] = len(el.find_all(kind))
                counts["li"] -= 100

                content_length = len(self._text(el)) # Count the text length excluding any surrounding whitespace
                link_density = self._get_link_density(el)
                to_remove = False
                reason = ""

                if counts["img"] > counts["p"]:
                    reason = "too many images"
                    to_remove = True
                elif counts["li"] > counts["p"] and name != "ul" and name != "ol":
                    reason = "more <li>s than <p>s"
                    to_remove = True
                elif counts["input"] > (counts["p"] / 3):
                    reason = "less than 3x <p>s than <input>s"
                    to_remove = True
                elif content_length < (self.options.get('min_text_length', self.TEXT_LENGTH_THRESHOLD)) and (counts["img"] == 0 or counts["img"] > 2):
                    reason = "too short a content length without a single image"
                    to_remove = True
                elif weight < 25 and link_density > 0.2:
                    reason = "too many links for its weight (#{weight})"
                    to_remove = True
                elif weight >= 25 and link_density > 0.5:
                    reason = "too many links for its weight (#{weight})"
                    to_remove = True
                elif (counts["embed"] == 1 and content_length < 75) or counts["embed"] > 1:
                    reason = "<embed>s with too short a content length, or too many <embed>s"
                    to_remove = True

                if to_remove:
                    self.debug("Conditionally cleaned %s#%s.%s with weight %s and content score %s because it has %s." %
                        (el.name, el.get('id',''), el.get('class', ''), weight, content_score, reason))
                    el.extract()

        for el in ([node] + node.find_all()):
            if not (self.options.get('attributes')):
                el.attrMap = {}

        return unicode(node)
            
    def extract(self):
        try:
            ruthless = True
            while True:
                self.preprocess(force=True)
                for tag in self._tags(self._html, 'script', 'style'):
                    tag.extract()
                    
                if ruthless:
                    self._remove_unlikely_candidates()
                self._transform_misused_divs_into_p()
                candidates = self._score_paragraphs(self.options.get('min_text_length'))
                best_candidate = self._select_best_candidate(candidates)
                if best_candidate:
                    article = self._get_article(candidates, best_candidate)
                else:
                    if ruthless:
                        ruthless = False
                        self.debug("ended up stripping too much - going for a safer parse")
                        # try again
                        continue
                    else:
                        article = self._html.find('body') or self._html
                        
                cleaned_article = self._sanitize(article, candidates)
                retry_length = self.options.get('retry_length') or self.RETRY_LENGTH
                of_acceptable_length = len(cleaned_article or '') >= retry_length
                if ruthless and not of_acceptable_length:
                    ruthless = False
                    continue # try again
                else:
                    return cleaned_article
                
        except Exception, e:
            self.logger.exception(e)
            if self.on_debug:
                raise e